package hystrix;

/**
 * Created by shifeifei on 2020/3/28.
 */
public class Test {

    public static void main(String[] args) throws Exception {
        OrderService service = new OrderService();
        QueryOrderCommand command = new QueryOrderCommand();
        command.setOrderService(service);
        command.queue();
    }
}
