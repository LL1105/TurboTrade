/*
 * Copyright 2019 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exchange.core.common.constant;

public enum MatcherEventType {

    // 用于表示标准的交易事件，记录成功的交易或订单状态更新（例如修改订单）
    TRADE,

    // 反映市场流动性不足导致的订单拒绝情况，对于市场订单尤为重要，帮助交易系统处理无法执行的订单
    REJECT,

    // 当订单被取消或减少时，系统需要释放相关的资金或保证金，确保风险管理机制能正常工作
    REDUCE,

    // 提供了一个灵活的方式来附加自定义的数据，用于处理一些特殊的事件或需求，不限于标准的交易模式
    BINARY_EVENT
}
