package thread;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


class MyCallable implements Callable<String> {
    private String message;

    public MyCallable(String message) {
        this.message = message;
    }

    @Override
    public String call() throws Exception {
        Thread.sleep(50);
        return "Processed: " + message;
    }
}


public class ImplementsCallableFuture {

    public static void main(String[] args) {

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        Callable<String> callableTask = new MyCallable("Hello, World!");

        try {
            Future<String> future = executorService.submit(callableTask);

            String result = future.get();
            System.out.println("Result: " + result);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            executorService.shutdown();
        }
    }
}
