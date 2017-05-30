# encoding: UTF-8

"""
DualThrust交易策略
"""

from datetime import time

from ctaBase import *
from ctaTemplate import CtaTemplate
import numpy as np
import talib


########################################################################
class MACDStrategy(CtaTemplate):
    """DualThrust交易策略"""
    className = 'MACDStrategy'
    author = u'Lizard'

    # 策略参数
    atrLength = 22  # 计算ATR指标的窗口数
    atrMaLength = 10  # 计算ATR均线的窗口数
    rsiLength = 5  # 计算RSI的窗口数
    rsiEntry = 16  # RSI的开仓信号
    trailingPercent = 5  # 百分比移动止损
    initDays = 10  # 初始化数据所用的天数
    fixedSize = 1  # 每次交易的数量

    L1 = 50  # 第一个均线参数
    L2 = 120  # 第二个均线参数
    fiveBar = None
    # 策略变量
    bar = None  # K线对象
    barMinute = EMPTY_STRING  # K线当前的分钟

    bufferSize = 150  # 需要缓存的数据的大小
    bufferCount = 0  # 目前已经缓存了的数据的计数
    highArray = np.zeros(bufferSize)  # K线最高价的数组
    lowArray = np.zeros(bufferSize)  # K线最低价的数组
    closeArray = np.zeros(bufferSize)  # K线收盘价的数组

    atrCount = 0  # 目前已经缓存了的ATR的计数
    atrArray = np.zeros(bufferSize)  # ATR指标的数组
    atrValue = 0  # 最新的ATR指标数值
    atrMa = 0  # ATR移动平均的数值

    rsiValue = 0  # RSI指标的数值
    rsiBuy = 0  # RSI买开阈值
    rsiSell = 0  # RSI卖开阈值
    intraTradeHigh = 0  # 移动止损用的持仓期内最高价
    intraTradeLow = 0  # 移动止损用的持仓期内最低价

    orderList = []  # 保存委托代码的列表

    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol',
                 'atrLength',
                 'atrMaLength',
                 'rsiLength',
                 'rsiEntry',
                 'trailingPercent',
                 'L1',
                 'L2']

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos',
               'atrValue',
               'atrMa',
               'rsiValue',
               'rsiBuy',
               'rsiSell']

    # ----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(MACDStrategy, self).__init__(ctaEngine, setting)

        self.barList = []

    # ----------------------------------------------------------------------
    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略初始化' % self.name)
        self.barConut = 0
        self.includeMin = 0
        # 载入历史数据，并采用回放计算的方式初始化策略数值
        initData = self.loadBar(self.initDays)
        for bar in initData:
            self.onBar(bar)

        self.putEvent()

    # ----------------------------------------------------------------------
    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略启动' % self.name)
        self.putEvent()

    # ----------------------------------------------------------------------
    def onStop(self):
        """停止策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略停止' % self.name)
        self.putEvent()

    # ----------------------------------------------------------------------
    def onTick(self, tick):
        """收到行情TICK推送（必须由用户继承实现）"""
        # 计算K线
        tickMinute = tick.datetime.minute

        if tickMinute != self.barMinute:
            if self.bar:
                self.onBar(self.bar)

            bar = CtaBarData()
            bar.vtSymbol = tick.vtSymbol
            bar.symbol = tick.symbol
            bar.exchange = tick.exchange

            bar.open = tick.lastPrice
            bar.high = tick.lastPrice
            bar.low = tick.lastPrice
            bar.close = tick.lastPrice

            bar.date = tick.date
            bar.time = tick.time
            bar.datetime = tick.datetime  # K线的时间设为第一个Tick的时间

            self.bar = bar  # 这种写法为了减少一层访问，加快速度
            self.barMinute = tickMinute  # 更新当前的分钟
        else:  # 否则继续累加新的K线
            bar = self.bar  # 写法同样为了加快速度

            bar.high = max(bar.high, tick.lastPrice)
            bar.low = min(bar.low, tick.lastPrice)
            bar.close = tick.lastPrice

    # ----------------------------------------------------------------------
    def onBar(self, bar):
        """收到Bar推送（必须由用户继承实现）"""
        # 如果当前是一个5分钟走完
        if bar.datetime.minute == 0:
        #if self.includeMin >= 240:
            # 如果已经有聚合5分钟K线
            if self.fiveBar:
                # 将最新分钟的数据更新到目前5分钟线中
                fiveBar = self.fiveBar
                fiveBar.high = max(fiveBar.high, bar.high)
                fiveBar.low = min(fiveBar.low, bar.low)
                fiveBar.close = bar.close
                self.includeMin +=1
                # 推送5分钟线数据
                self.onFiveBar(fiveBar)

                # 清空5分钟线数据缓存
                self.fiveBar = None
                self.includeMin = 0
        else:
            # 如果没有缓存则新建
            if not self.fiveBar:
                fiveBar = CtaBarData()

                fiveBar.vtSymbol = bar.vtSymbol
                fiveBar.symbol = bar.symbol
                fiveBar.exchange = bar.exchange

                fiveBar.open = bar.open
                fiveBar.high = bar.high
                fiveBar.low = bar.low
                fiveBar.close = bar.close

                fiveBar.date = bar.date
                fiveBar.time = bar.time
                fiveBar.datetime = bar.datetime

                self.fiveBar = fiveBar

            else:
                fiveBar = self.fiveBar
                fiveBar.high = max(fiveBar.high, bar.high)
                fiveBar.low = min(fiveBar.low, bar.low)
                fiveBar.close = bar.close
                self.includeMin += 1

    def onFiveBar(self, bar):
        """收到Bar推送（必须由用户继承实现）"""
        # 撤销之前发出的尚未成交的委托（包括限价单和停止单）
        for orderID in self.orderList:
            self.cancelOrder(orderID)
        self.orderList = []

        # 保存K线数据
        self.closeArray[0:self.bufferSize - 1] = self.closeArray[1:self.bufferSize]
        self.highArray[0:self.bufferSize - 1] = self.highArray[1:self.bufferSize]
        self.lowArray[0:self.bufferSize - 1] = self.lowArray[1:self.bufferSize]

        self.closeArray[-1] = bar.close
        self.highArray[-1] = bar.high
        self.lowArray[-1] = bar.low

        self.bufferCount += 1
        if self.bufferCount < self.bufferSize:
            return

        # 计算指标数值
        self.MACDValue, self.avgMACDValue, self.MACDDiffValue, = talib.MACD(self.closeArray, fastperiod=12,
                                                                            slowperiod=26, signalperiod=9)
        self.MA1 = talib.MA(self.closeArray, timeperiod=self.L1)
        self.MA2 = talib.MA(self.closeArray, timeperiod=self.L2)

        # 判断是否要进行交易

        # 当前无仓位
        if self.pos == 0:
            self.intraTradeHigh = bar.high
            self.intraTradeLow = bar.low

            if self.MACDValue[-1] > 0 and self.MA1[-1] > self.MA2[-1] and self.MACDDiffValue[-1] > 0 and \
                            self.closeArray[-1] > self.MA1[-1] and self.closeArray[-1] > self.MA2[-1]:
                self.buy(bar.close, self.fixedSize)
            elif self.MACDValue[-1] < 0 and self.MA1[-1] < self.MA2[-1] and self.MACDDiffValue[-1] < 0 and \
                            self.closeArray[-1] < self.MA1[-1] and self.closeArray[-1] < self.MA2[-1]:
                self.short(bar.close, self.fixedSize)

        # 持有多头仓位
        elif self.pos > 0:
            if self.MACDValue[-1] < 0 and self.MA1[-1] < self.MA2[-1]:
                self.sell(bar.close, self.fixedSize)
            else:
                # 计算多头持有期内的最高价，以及重置最低价
                self.intraTradeHigh = max(self.intraTradeHigh, bar.high)
                self.intraTradeLow = bar.low
                # 计算多头移动止损
                longStop = self.intraTradeHigh * (1 - self.trailingPercent / 100)
                # 发出本地止损委托，并且把委托号记录下来，用于后续撤单
                orderID = self.sell(longStop, abs(self.pos), stop=True)
                self.orderList.append(orderID)

        # 持有空头仓位
        elif self.pos < 0:
            if self.MACDValue[-1] > 0 and self.MA1[-1] > self.MA2[-1]:
                self.cover(bar.close, self.fixedSize)
            else:
                self.intraTradeLow = min(self.intraTradeLow, bar.low)
                self.intraTradeHigh = bar.high

                shortStop = self.intraTradeLow * (1 + self.trailingPercent / 100)
                orderID = self.cover(shortStop, abs(self.pos), stop=True)
                self.orderList.append(orderID)

        # 发出状态更新事件
        self.putEvent()

    # ----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        pass

    # ----------------------------------------------------------------------
    def onTrade(self, trade):
        # 发出状态更新事件
        self.putEvent()


if __name__ == '__main__':
    # 提供直接双击回测的功能
    # 导入PyQt4的包是为了保证matplotlib使用PyQt4而不是PySide，防止初始化出错
    from ctaBacktesting import *

    # from PyQt5 import QtCore, QtGui

    # 创建回测引擎
    engine = BacktestingEngine()

    # 设置引擎的回测模式为K线
    engine.setBacktestingMode(engine.BAR_MODE)

    # 设置回测用的数据起始日期
    engine.setStartDate('20130501')

    # 设置产品相关参数
    engine.setSlippage(0.2)  # 股指1跳
    engine.setRate(0.3 / 10000)  # 万0.3
    engine.setSize(300)  # 股指合约大小
    engine.setPriceTick(0.2)  # 股指最小价格变动

    # 设置使用的历史数据库
    engine.setDatabase(MINUTE_DB_NAME, 'IF0000')

    # 在引擎中创建策略对象
    engine.initStrategy(MACDStrategy, {})

    # 开始跑回测
    engine.runBacktesting()

    # 显示回测结果
    engine.showBacktestingResult()

    ## 跑优化
    # setting = OptimizationSetting()                 # 新建一个优化任务设置对象
    # setting.setOptimizeTarget('capital')            # 设置优化排序的目标是策略净盈利
    # setting.addParameter('atrLength', 12, 20, 2)    # 增加第一个优化参数atrLength，起始11，结束12，步进1
    # setting.addParameter('atrMa', 20, 30, 5)        # 增加第二个优化参数atrMa，起始20，结束30，步进1
    # setting.addParameter('rsiLength', 5)            # 增加一个固定数值的参数

    ## 性能测试环境：I7-3770，主频3.4G, 8核心，内存16G，Windows 7 专业版
    ## 测试时还跑着一堆其他的程序，性能仅供参考
    # import time
    # start = time.time()

    ## 运行单进程优化函数，自动输出结果，耗时：359秒
    # engine.runOptimization(AtrRsiStrategy, setting)

    ## 多进程优化，耗时：89秒
    ##engine.runParallelOptimization(AtrRsiStrategy, setting)

    # print u'耗时：%s' %(time.time()-start)
