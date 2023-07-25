package hk.cykenleung.example;

import java.util.*;
import java.util.concurrent.*;

class Main {
    public static void main(String[] args) {
        List<Long> nums = new ArrayList<>();
        for (long i = 1; i <= 50000; i++) {
            nums.add(i);
        }
        try {
            System.out.println(parallelSum(nums));
            System.out.println((1 + 50000L) * 50000L / 2);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static long parallelSum(List<Long> nums) throws InterruptedException, ExecutionException {
        int taskSize = 1000;
        int batchCount = (int) Math.ceil(nums.size() / taskSize);
        ExecutorService executor = Executors.newFixedThreadPool(20);
        CompletionService<Long> ecs = new ExecutorCompletionService<>(executor);
        for (int i = 0; i < batchCount; i++) {
            int start = i * batchCount;
            ecs.submit(new SumWorker(nums, start, start + taskSize));
        }
        long sum = 0;
        for (int i = 0; i < batchCount; i++) {
            sum += ecs.take().get();
        }
        return sum;
    }
}

class SumWorker implements Callable<Long> {
    private int start;
    private int end;
    private List<Long> nums;

    public SumWorker(List<Long> nums, int start, int end) {
        this.nums = nums;
        this.start = start;
        this.end = end < nums.size() ? end : nums.size() - 1;
    }

    @Override
    public Long call() {
        System.out.printf("Start %s...\n", Thread.currentThread().getId());
        long sum = 0;
        for (int i = start; i <= end; i++) {
            sum += nums.get(i);
        }
        System.out.printf("End %s...\n", Thread.currentThread().getId());
        return sum;
    }
}