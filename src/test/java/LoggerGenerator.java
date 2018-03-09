
import org.apache.log4j.Logger;

import java.util.Random;

/**
 * Created by Horizon
 * Time: 下午9:44 2018/2/21
 * Description:  模拟日志产生
 */
public class LoggerGenerator {

    private static Logger logger = Logger.getLogger(LoggerGenerator.class);
    private static Random random = new Random();
    public static void main(String[] args) throws InterruptedException {
        int index = 0;

        while (true) {
            Thread.sleep(1000);
            logger.info("values: " + random.nextInt(10000));
        }

    }
}
