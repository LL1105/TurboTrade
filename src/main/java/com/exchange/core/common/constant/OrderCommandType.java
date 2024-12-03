package com.exchange.core.common.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.HashMap;

@Getter
@AllArgsConstructor
public enum OrderCommandType {
    // 订单相关命令
    PLACE_ORDER((byte) 1, true),    // 下单命令，代码为1，状态会发生变化
    CANCEL_ORDER((byte) 2, true),   // 取消订单命令，代码为2，状态会发生变化
    MOVE_ORDER((byte) 3, true),     // 移动订单命令，代码为3，状态会发生变化
    REDUCE_ORDER((byte) 4, true),   // 减少订单命令，代码为4，状态会发生变化

    // 订单簿相关命令
    ORDER_BOOK_REQUEST((byte) 6, false),  // 订单簿请求，代码为6，不会改变状态

    // 用户管理命令
    ADD_USER((byte) 10, true),           // 添加用户命令，代码为10，状态会发生变化
    BALANCE_ADJUSTMENT((byte) 11, true), // 余额调整命令，代码为11，状态会发生变化
    SUSPEND_USER((byte) 12, true),       // 暂停用户命令，代码为12，状态会发生变化
    RESUME_USER((byte) 13, true),        // 恢复用户命令，代码为13，状态会发生变化

    // 二进制数据命令
    BINARY_DATA_QUERY((byte) 90, false),  // 二进制数据查询命令，代码为90，不会改变状态
    BINARY_DATA_COMMAND((byte) 91, true), // 二进制数据命令，代码为91，状态会发生变化

    // 状态持久化命令
    PERSIST_STATE_MATCHING((byte) 110, true),  // 匹配引擎状态持久化命令，代码为110，状态会发生变化
    PERSIST_STATE_RISK((byte) 111, true),      // 风控状态持久化命令，代码为111，状态会发生变化

    // 控制命令
    GROUPING_CONTROL((byte) 118, false), // 分组控制命令，代码为118，不会改变状态
    NOP((byte) 120, false),              // 空操作命令，代码为120，不会改变状态
    RESET((byte) 124, true),             // 重置命令，代码为124，状态会发生变化
    SHUTDOWN_SIGNAL((byte) 127, false),  // 关机信号命令，代码为127，不会改变状态

    // 保留的压缩命令
    RESERVED_COMPRESSED((byte) -1, false); // 保留的压缩命令，代码为-1，不会改变状态

    private final byte code;  // 每种命令的唯一代码
    private final boolean mutate;  // 是否会改变系统状态

    // 根据命令代码获取对应的 OrderCommandType 枚举
    public static OrderCommandType fromCode(byte code) {
        // TODO: 可以尝试使用 if-else 代替
        final OrderCommandType result = codes.get(code);
        if (result == null) {
            throw new IllegalArgumentException("未知的订单命令类型代码:" + code);
        }
        return result;
    }

    // 存储所有命令类型代码和对应命令类型的映射
    private static HashMap<Byte, OrderCommandType> codes = new HashMap<>();

    static {
        // 初始化命令类型代码与枚举常量的映射关系
        for (OrderCommandType x : values()) {
            codes.put(x.code, x);
        }
    }
}
