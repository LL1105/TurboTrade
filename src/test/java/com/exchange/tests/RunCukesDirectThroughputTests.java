package com.exchange.tests;

import static io.cucumber.junit.platform.engine.Constants.PLUGIN_PROPERTY_NAME;

import com.exchange.core.common.config.PerformanceConfiguration;
import com.exchange.tests.steps.OrderStepdefs;
import io.cucumber.plugin.EventListener;
import io.cucumber.plugin.event.EventPublisher;
import io.cucumber.plugin.event.TestRunFinished;
import io.cucumber.plugin.event.TestRunStarted;
import lombok.extern.slf4j.Slf4j;
import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.ConfigurationParameters;
import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.SelectClasspathResource;
import org.junit.platform.suite.api.SelectClasspathResources;
import org.junit.platform.suite.api.Suite;

@Suite  // JUnit 5的Suite注解，表示这是一个测试套件
@IncludeEngines("cucumber")  // 指定该套件使用Cucumber引擎来执行
@SelectClasspathResources({  // 选择要执行的Cucumber特性文件
    @SelectClasspathResource("exchange/core2/tests/features/basic.feature"),  // 选择basic.feature文件
    @SelectClasspathResource("exchange/core2/tests/features/risk.feature")    // 选择risk.feature文件
})
@ConfigurationParameters({  // 配置参数
    @ConfigurationParameter(key = PLUGIN_PROPERTY_NAME, value = "pretty, html:target/cucumber/cucumber.html, exchange.core2.tests.RunCukesDirectThroughputTests$CukeNaiveLifeCycleHandler"),
    // 配置Cucumber插件，输出格式为'pretty'和HTML报告，并设置生命周期处理类
})
@Slf4j
public class RunCukesDirectThroughputTests {

    // 内部类：Cucumber生命周期处理器，用于在测试运行开始和结束时执行操作
    public static class CukeNaiveLifeCycleHandler implements EventListener {

        @Override
        public void setEventPublisher(EventPublisher eventPublisher) {
            // 在测试运行开始时，设置性能配置为吞吐量性能测试配置
            eventPublisher.registerHandlerFor(TestRunStarted.class,
                event -> OrderStepdefs.testPerformanceConfiguration = PerformanceConfiguration.throughputPerformanceBuilder()
                    .build());
            
            // 在测试运行结束时，清除性能配置
            eventPublisher.registerHandlerFor(TestRunFinished.class,
                event -> OrderStepdefs.testPerformanceConfiguration = null);
        }
    }
}
