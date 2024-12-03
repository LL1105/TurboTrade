package com.exchange.core.common.constant;

import lombok.Getter;

import java.util.Arrays;

@Getter
public enum CommandResultCode {
    // 新订单状态
    NEW(0),  // 新订单，代码为0

    // 匹配引擎相关状态
    VALID_FOR_MATCHING_ENGINE(1),  // 订单有效，等待匹配引擎处理，代码为1

    // 成功与接受状态
    SUCCESS(100),  // 操作成功，代码为100
    ACCEPTED(110), // 操作被接受，代码为110

    // 授权相关错误
    AUTH_INVALID_USER(-1001),  // 无效用户，代码为-1001
    AUTH_TOKEN_EXPIRED(-1002), // 授权令牌已过期，代码为-1002

    // 订单相关错误
    INVALID_SYMBOL(-1201),  // 无效的交易对符号，代码为-1201
    INVALID_PRICE_STEP(-1202),  // 无效的价格步长，代码为-1202
    UNSUPPORTED_SYMBOL_TYPE(-1203), // 不支持的交易对类型，代码为-1203

    // 风控相关错误
    RISK_NSF(-2001),  // 风控错误：资金不足，代码为-2001
    RISK_INVALID_RESERVE_BID_PRICE(-2002),  // 风控错误：无效的保留竞标价格，代码为-2002
    RISK_ASK_PRICE_LOWER_THAN_FEE(-2003),  // 风控错误：询价价格低于费用，代码为-2003
    RISK_MARGIN_TRADING_DISABLED(-2004),  // 风控错误：禁止保证金交易，代码为-2004

    // 匹配引擎相关错误
    MATCHING_UNKNOWN_ORDER_ID(-3002),  // 未知的订单ID，代码为-3002
    MATCHING_UNSUPPORTED_COMMAND(-3004),  // 不支持的命令，代码为-3004
    MATCHING_INVALID_ORDER_BOOK_ID(-3005),  // 无效的订单簿ID，代码为-3005
    MATCHING_MOVE_FAILED_PRICE_OVER_RISK_LIMIT(-3041),  // 移动订单失败，价格超过风险限制，代码为-3041
    MATCHING_REDUCE_FAILED_WRONG_SIZE(-3051),  // 减少订单失败，订单大小错误，代码为-3051

    // 用户管理相关错误
    USER_MGMT_USER_ALREADY_EXISTS(-4001),  // 用户已存在，代码为-4001

    // 用户账户余额调整相关错误
    USER_MGMT_ACCOUNT_BALANCE_ADJUSTMENT_ALREADY_APPLIED_SAME(-4101),  // 账户余额调整已应用相同，代码为-4101
    USER_MGMT_ACCOUNT_BALANCE_ADJUSTMENT_ALREADY_APPLIED_MANY(-4102),  // 账户余额调整已应用多次，代码为-4102
    USER_MGMT_ACCOUNT_BALANCE_ADJUSTMENT_NSF(-4103),  // 账户余额调整资金不足，代码为-4103
    USER_MGMT_NON_ZERO_ACCOUNT_BALANCE(-4104),  // 用户账户余额不为零，代码为-4104

    // 用户暂停相关错误
    USER_MGMT_USER_NOT_SUSPENDABLE_HAS_POSITIONS(-4130),  // 用户无法暂停，仍有持仓，代码为-4130
    USER_MGMT_USER_NOT_SUSPENDABLE_NON_EMPTY_ACCOUNTS(-4131),  // 用户无法暂停，账户不为空，代码为-4131
    USER_MGMT_USER_NOT_SUSPENDED(-4132),  // 用户尚未暂停，代码为-4132
    USER_MGMT_USER_ALREADY_SUSPENDED(-4133),  // 用户已经暂停，代码为-4133

    USER_MGMT_USER_NOT_FOUND(-4201),  // 用户未找到，代码为-4201

    // 交易对管理相关错误
    SYMBOL_MGMT_SYMBOL_ALREADY_EXISTS(-5001),  // 交易对已存在，代码为-5001

    // 二进制命令相关错误
    BINARY_COMMAND_FAILED(-8001),  // 二进制命令失败，代码为-8001
    REPORT_QUERY_UNKNOWN_TYPE(-8003),  // 报告查询未知类型，代码为-8003

    // 状态持久化相关错误
    STATE_PERSIST_RISK_ENGINE_FAILED(-8010),  // 风控引擎状态持久化失败，代码为-8010
    STATE_PERSIST_MATCHING_ENGINE_FAILED(-8020),  // 匹配引擎状态持久化失败，代码为-8020

    // 丢弃状态
    DROP(-9999);  // 丢弃，代码为-9999

    // 代码低于-10000预留给网关

    private int code;  // 命令结果代码

    // 构造函数，初始化命令结果代码
    CommandResultCode(int code) {
        this.code = code;
    }

    /**
     * 合并多个命令执行结果，返回第一个失败的结果
     *
     * @param results 要合并的命令执行结果
     * @return 如果存在失败的结果，返回第一个失败的结果；如果都成功，则返回SUCCESS；如果只有接受，返回ACCEPTED
     */
    public static CommandResultCode mergeToFirstFailed(CommandResultCode... results) {
        // 查找第一个失败的命令结果，忽略成功和已接受的结果
        return Arrays.stream(results)
                .filter(r -> r != SUCCESS && r != ACCEPTED)
                .findFirst()
                .orElse(Arrays.stream(results).anyMatch(r -> r == SUCCESS) ? SUCCESS : ACCEPTED);
    }

}
