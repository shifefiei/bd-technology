package hystrix;

import com.netflix.hystrix.*;

/**
 * Created by shifeifei on 2020/3/28
 * <p>
 * https://www.cnblogs.com/xinzhao/p/11398534.html
 * https://www.cnblogs.com/duanxz/p/10949816.html
 */

public class QueryOrderCommand extends HystrixCommand<Integer> {

    private OrderService orderService;


    public QueryOrderCommand() {

        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("orderService"))

                .andCommandKey(HystrixCommandKey.Factory.asKey("queryByOrderId"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("order-service-pool"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        //至少有10个请求，熔断器才进行错误率的计算
                        .withCircuitBreakerRequestVolumeThreshold(5)
                        //错误率达到50开启熔断保护
                        .withCircuitBreakerErrorThresholdPercentage(50)
                        //熔断器中断请求5秒后会进入半打开状态,放部分流量过去重试
                        .withCircuitBreakerSleepWindowInMilliseconds(5000)
                        .withExecutionTimeoutEnabled(true)
                        // 设置超时时间
                        .withExecutionTimeoutInMilliseconds(20000)
                        // 设置fallback最大请求并发数
                        .withFallbackIsolationSemaphoreMaxConcurrentRequests(30)
                        .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.THREAD))

                .andThreadPoolPropertiesDefaults(
                        HystrixThreadPoolProperties.Setter()
                                .withCoreSize(10)
                                // 设置等待队列大小为10
                                .withMaximumSize(10)
                                .withAllowMaximumSizeToDivergeFromCoreSize(true)
                                .withMaxQueueSize(30)
                                .withQueueSizeRejectionThreshold(30)
                ));//最大并发请求量

    }

    @Override
    protected Integer run() {
        return orderService.queryByOrderId();
    }

    @Override
    protected Integer getFallback() {
        return -1;
    }

    public OrderService getOrderService() {
        return orderService;
    }

    public void setOrderService(OrderService orderService) {
        this.orderService = orderService;
    }
}
