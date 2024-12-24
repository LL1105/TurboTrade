package com.exchange.tests.util;

import com.exchange.core.common.CoreSymbolSpecification;
import com.exchange.core.common.constant.SymbolType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.distribution.ParetoDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;

import java.util.*;

/**
 * 用户货币账户生成器，生成随机用户和他们应该拥有的货币账户。
 * 用于模拟不同用户在多个货币市场中的账户情况。
 */
@Slf4j
public final class UserCurrencyAccountsGenerator {

    /**
     * 生成随机用户及其拥有的货币账户，每个用户的账户总数将在 `accountsToCreate` 和
     * `accountsToCreate + currencies.size()` 之间。
     * <p>
     * 平均而言，每个用户会有 4 个账户（在 1 到 currencies.size() 之间）。
     *
     * @param accountsToCreate 要创建的用户账户总数
     * @param currencies       支持的货币类型
     * @return 包含每个用户账户信息的列表，每个用户的账户是一个 `BitSet`
     */
    public static List<BitSet> generateUsers(final int accountsToCreate, Collection<Integer> currencies) {
        log.debug("生成用户，账户总数为 {} (货币种类数: {})...", accountsToCreate, currencies.size());
        final ExecutionTime executionTime = new ExecutionTime();
        final List<BitSet> result = new ArrayList<>();
        result.add(new BitSet()); // uid=0 没有账户

        final Random rand = new Random(1);

        // 使用帕累托分布来控制每个用户账户数
        final RealDistribution paretoDistribution = new ParetoDistribution(new JDKRandomGenerator(0), 1, 1.5);
        final int[] currencyCodes = currencies.stream().mapToInt(i -> i).toArray();

        int totalAccountsQuota = accountsToCreate;
        do {
            // TODO 可以在此处偏好选择某些货币
            // 根据帕累托分布来决定每个用户开设的账户数量
            final int accountsToOpen = Math.min(Math.min(1 + (int) paretoDistribution.sample(), currencyCodes.length), totalAccountsQuota);
            final BitSet bitSet = new BitSet();
            do {
                // 随机选择一个货币类型
                final int currencyCode = currencyCodes[rand.nextInt(currencyCodes.length)];
                bitSet.set(currencyCode);
            } while (bitSet.cardinality() != accountsToOpen);

            totalAccountsQuota -= accountsToOpen;
            result.add(bitSet);

        } while (totalAccountsQuota > 0);

        log.debug("生成了 {} 个用户，共 {} 个账户，最多包含 {} 种货币，耗时 {}", result.size(), accountsToCreate, currencies.size(), executionTime.getTimeFormatted());
        return result;
    }

    /**
     * 根据符号要求，为指定的货币符号选择适合的用户。
     *
     * @param users2currencies 用户与货币账户映射的列表
     * @param spec             符号规格，包含基础货币和报价货币信息
     * @param symbolMessagesExpected 预计生成的符号消息数量
     * @return 选择的用户列表，返回的是符合符号要求的用户 ID 数组
     */
    public static int[] createUserListForSymbol(final List<BitSet> users2currencies, final CoreSymbolSpecification spec, int symbolMessagesExpected) {

        // 根据符号消息的数量，选择大约相同数量的用户进行测试
        // 至少需要选择 2 个用户，但不能超过用户总数的一半
        int numUsersToSelect = Math.min(users2currencies.size(), Math.max(2, symbolMessagesExpected / 5));

        final ArrayList<Integer> uids = new ArrayList<>();
        final Random rand = new Random(spec.symbolId);
        int uid = 1 + rand.nextInt(users2currencies.size() - 1);
        int c = 0;
        do {
            BitSet accounts = users2currencies.get(uid);
            // 符合要求的用户必须拥有符合条件的货币账户
            if (accounts.get(spec.quoteCurrency) && (spec.type == SymbolType.FUTURES_CONTRACT || accounts.get(spec.baseCurrency))) {
                uids.add(uid);
            }
            if (++uid == users2currencies.size()) {
                uid = 1;
            }
            c++;
        } while (uids.size() < numUsersToSelect && c < users2currencies.size());

        // 返回符合条件的用户 ID 数组
        return uids.stream().mapToInt(x -> x).toArray();
    }
}
