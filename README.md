# TurboTrade

`注：该项目仅供学习使用`

高性能内存交易撮合引擎。

## 项目相关术语
- `Order Book(订单簿)`：用于存储未成交的订单，分为买单和卖单
- `Symbol`：金融工具的简短代码或标识符，用于区分交易市场中的不同产品。(例如：在虚拟货币中，BTC/USDT交易对就代表一个Symbol)
- `Order(订单)`：表示用户提交的交易请求(买入/卖出)
- `Limit Order(限价单)`：指定价格和数量，在指定价格范围内，如果能成交，则成交，否则挂单。
- `Market Order(市价单)`：未指定价格，以当前价格成交，未成交的订单会被挂单。
- `Symbol Type(交易对类型)`:
  - `CURRENCY_EXCHANGE_PAIR(货币兑换交易对)`：通常用于现货市场，涉及一种货币或资产兑换另一种货币
  - `FUTURES_CONTRACT(期权合约)`：是一种标准化的合约，买卖双方约定在未来的某个日期以特定的价格交易某种资产
  - `OPTION(期权)`：一种衍生品合约，赋予买方在特定时间内以指定价格买入或卖出标的资产的权利，但没有义务
- `Order Type`:
  - `GTC (Good till Cancel)`：表示订单一直有效，直到主动取消为止（或成交完成）
    常用于普通限价订单，不受时间约束
  - `IOC (Immediate or Cancel)`：表示订单必须立即执行其全部或部分（能成交的部分），未成交的部分会被自动取消
  - `IOC_BUDGET`：与 IOC 类似，但增加了总金额上限。订单不会超过设定的金额进行交易
  - `FOK (Fill or Kill)`：表示订单必须立即全部成交，否则订单将被自动取消
  - `FOK_BUDGET`：与 FOK 类似，但增加了总金额上限，使得成交金额不会超过预算
- `Taker订单(接收单)`：表示主动进行匹配的订单
- `Maker订单(挂单)`：表示在订单簿中等待被匹配的订单

## 撮合引擎责任
撮合引擎负责接收订单，并执行订单的撮合。撮合引擎会根据订单的属性（价格、数量、订单类型等）来确定订单的优先级，并执行订单的撮合。

撮合引擎最常见的输入有：
- `PlaceOrder`: 用于接收用户提交的订单，然后根据撮合算法对订单进行撮合
- `CancelOrder`: 用于接收用户取消订单的请求，如果订单还没有成交，则取消订单，即开口订单
- `MoveOrder`：修改订单请求，例如修改订单价格等
- `ReduceOrder`：减少订单请求，例如减少订单数量等
- `OrderBookRequest`：请求查看当前订单簿信

撮合引擎应当只负责自己负责的一部分功能，也就是一个黑盒子。这样撮合引擎可以被广泛用在不同的交易撮合场景中。

## 整体设计
### 设计目标
1. 撮合引擎应当独立为一个模块，与外部系统松耦合。
2. 撮合引擎应该使用内存撮合的方式，以达到高性能的目的。
3. 撮合引擎应当使用事件驱动的方式，以减少线程相关资源的消耗。
4. 撮合引擎应当使用高效的数据结构，以减少内存消耗。
5. 撮合引擎应当提供快照或重做日志的功能，避免服务宕机导致数据丢失。
6. 撮合引擎应当具有高可用性，在机器故障时能够快速恢复提供正常的服务。
### 技术选型
1. 本项目采用纯内存撮合方式，避免网络及其数据库I/O带来的性能损耗。
2. 本项目采用`Disruptor`框架作为事件驱动框架，以减少线程资源消耗。
3. 本项目使用`OpenHFT Chronicle-Wire`高性能数据序列化框架，提升数据序列化/反序列化效率。
4. 本项目使用`Adaptive Radix Trees`数据结构在内存中保存订单。
5. 本项目使用`LZ4`压缩算法对数据进行快速压缩。
6. 本项目使用`Raft`分布式协议保证撮合服务的高可用性。
7. 本项目使用`Real Logic Agrona`进行高效内存管理。

## 需求设计
### 整体设计

### 订单簿的储存
订单簿的存储采用`Adaptive Radix Tree`加对象池的方式，`ART`具有很高的插入和查找性能，并且有效的节省存储空间，对象池可以减少创建对象的开销，整体达到最极限的性能。

使用订单的价格作为key，类型为Long，value为一个订单桶，存储了相同价格的订单，并将它们以双向链表的结构存储。

使用订单的id作为key，类型为Long，value为一个订单对象，存储了订单的所有信息。
### 交易事件
