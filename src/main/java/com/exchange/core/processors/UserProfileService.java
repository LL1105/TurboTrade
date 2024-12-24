package com.exchange.core.processors;

import com.exchange.core.common.StateHash;
import com.exchange.core.common.UserProfile;
import com.exchange.core.common.constant.CommandResultCode;
import com.exchange.core.common.constant.UserStatus;
import com.exchange.core.utils.HashingUtils;
import com.exchange.core.utils.SerializationUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

/**
 * 用户资料服务，具有状态管理功能
 * <p>
 * TODO: 使其支持多实例
 */
@Slf4j
public final class UserProfileService implements WriteBytesMarshallable, StateHash {

    /*
     * 状态：uid 到 UserProfile 的映射
     */
    @Getter
    private final LongObjectHashMap<UserProfile> userProfiles;

    // 默认构造函数
    public UserProfileService() {
        this.userProfiles = new LongObjectHashMap<>(1024);
    }

    // 从字节流中构造 UserProfileService
    public UserProfileService(BytesIn bytes) {
        this.userProfiles = SerializationUtils.readLongHashMap(bytes, UserProfile::new);
    }

    /**
     * 查找用户资料
     *
     * @param uid 用户ID
     * @return 用户资料
     */
    public UserProfile getUserProfile(long uid) {
        return userProfiles.get(uid);
    }

    /**
     * 获取用户资料，如果用户不存在则将其状态设置为挂起
     *
     * @param uid 用户ID
     * @return 用户资料
     */
    public UserProfile getUserProfileOrAddSuspended(long uid) {
        return userProfiles.getIfAbsentPut(uid, () -> new UserProfile(uid, UserStatus.SUSPENDED));
    }

    /**
     * 执行余额调整操作
     *
     * @param uid                  用户ID
     * @param currency             币种
     * @param amount               余额调整金额
     * @param fundingTransactionId 资金交易ID（应递增）
     * @return 结果代码
     */
    public CommandResultCode balanceAdjustment(final long uid, final int currency, final long amount, final long fundingTransactionId) {

        final UserProfile userProfile = getUserProfile(uid);
        if (userProfile == null) {
            log.warn("未找到用户资料: {}", uid);
            return CommandResultCode.AUTH_INVALID_USER;
        }

        // 防止重复的资金交易应用
        if (userProfile.adjustmentsCounter == fundingTransactionId) {
            return CommandResultCode.USER_MGMT_ACCOUNT_BALANCE_ADJUSTMENT_ALREADY_APPLIED_SAME;
        }
        if (userProfile.adjustmentsCounter > fundingTransactionId) {
            return CommandResultCode.USER_MGMT_ACCOUNT_BALANCE_ADJUSTMENT_ALREADY_APPLIED_MANY;
        }

        // 验证余额是否足够进行取款
        if (amount < 0 && (userProfile.accounts.get(currency) + amount < 0)) {
            return CommandResultCode.USER_MGMT_ACCOUNT_BALANCE_ADJUSTMENT_NSF;
        }

        userProfile.adjustmentsCounter = fundingTransactionId;
        userProfile.accounts.addToValue(currency, amount);

        return CommandResultCode.SUCCESS;
    }

    /**
     * 创建一个新的用户资料
     *
     * @param uid 用户ID
     * @return 如果用户被成功添加，则返回 true
     */
    public boolean addEmptyUserProfile(long uid) {
        if (userProfiles.get(uid) == null) {
            userProfiles.put(uid, new UserProfile(uid, UserStatus.ACTIVE));
            return true;
        } else {
            log.debug("无法添加用户，用户已存在: {}", uid);
            return false;
        }
    }

    /**
     * 挂起用户资料以提高性能，挂起操作会从核心系统中移除用户资料。
     * 用户账户余额应先通过 BalanceAdjustmentType=SUSPEND 调整为零。
     * 挂起状态下不允许有未结算的仓位。
     * 但是，用户资料可以在某些情况下恢复，包括挂起前未处理的挂单或命令。
     *
     * @param uid 用户ID
     * @return 结果代码
     */
    public CommandResultCode suspendUserProfile(long uid) {
        final UserProfile userProfile = userProfiles.get(uid);
        if (userProfile == null) {
            return CommandResultCode.USER_MGMT_USER_NOT_FOUND;
        } else if (userProfile.userStatus == UserStatus.SUSPENDED) {
            return CommandResultCode.USER_MGMT_USER_ALREADY_SUSPENDED;
        } else if (userProfile.positions.anySatisfy(pos -> !pos.isEmpty())) {
            return CommandResultCode.USER_MGMT_USER_NOT_SUSPENDABLE_HAS_POSITIONS;
        } else if (userProfile.accounts.anySatisfy(acc -> acc != 0)) {
            return CommandResultCode.USER_MGMT_USER_NOT_SUSPENDABLE_NON_EMPTY_ACCOUNTS;
        } else {
            log.debug("已挂起用户资料: {}", userProfile);
            userProfiles.remove(uid);
            // TODO: 对 UserProfile 对象进行池化
            return CommandResultCode.SUCCESS;
        }
    }

    /**
     * 恢复挂起的用户资料
     *
     * @param uid 用户ID
     * @return 结果代码
     */
    public CommandResultCode resumeUserProfile(long uid) {
        final UserProfile userProfile = userProfiles.get(uid);
        if (userProfile == null) {
            // 创建一个新的空用户资料，余额调整操作应该稍后应用
            userProfiles.put(uid, new UserProfile(uid, UserStatus.ACTIVE));
            return CommandResultCode.SUCCESS;
        } else if (userProfile.userStatus != UserStatus.SUSPENDED) {
            // 尝试恢复非挂起账户（或多次恢复）
            return CommandResultCode.USER_MGMT_USER_NOT_SUSPENDED;
        } else {
            // 恢复现有的挂起资料（可以包含非空仓位或账户）
            userProfile.userStatus = UserStatus.ACTIVE;
            log.debug("已恢复用户资料: {}", userProfile);
            return CommandResultCode.SUCCESS;
        }
    }

    /**
     * 重置模块 - 仅用于测试
     */
    public void reset() {
        userProfiles.clear();
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        // 序列化 userProfiles
        SerializationUtils.marshallLongHashMap(userProfiles, bytes);
    }

    @Override
    public int stateHash() {
        return HashingUtils.stateHash(userProfiles);
    }
}
