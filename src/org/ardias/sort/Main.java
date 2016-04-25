package org.ardias.sort;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) throws InterruptedException, IOException {
	// write your code here
        String inputFileName = args[0];
        long start = System.nanoTime();
        File input = Paths.get(inputFileName).toFile();
        File output = Paths.get(input.getName() + ".sorted").toFile();
        //ExternalSort.main(new String[] {input.getName(), output.getName()});

        ExecutorService pool = Executors.newCachedThreadPool();
        LineSorter sorter = new LineSorter(input, output);
        try {
            sorter.sort(pool);
        } catch (IOException|InterruptedException e) {
            e.printStackTrace();
        } finally {
            pool.shutdown();
        }

        long duration = System.nanoTime() - start;
        System.out.println(String.format("Took %d seconds", TimeUnit.NANOSECONDS.toSeconds(duration)));
        pool.awaitTermination(10, TimeUnit.SECONDS);
    }
}
