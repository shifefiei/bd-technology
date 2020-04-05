package hystrix;

import io.netty.util.internal.ThreadLocalRandom;

import java.util.Random;

/**
 * Created by shifeifei on 2020/3/28.
 */
public class OrderService {

    public Integer queryByOrderId() {
        return ThreadLocalRandom.current().nextInt();
    }
}
