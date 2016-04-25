package org.ardias.sort;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Antonio Dias on 24/04/2016.
 */
public class LineSorter {

    private final LinkedBlockingQueue<String> writerQueue;
    private final File input;
    private final AtomicLong fileIndex = new AtomicLong(0);

    public LineSorter(File input, File output) {
        this.input = input;

        //nodes iterated in add order
        writerQueue = new LinkedBlockingQueue<>();
    }


    public void sort(ExecutorService pool) throws IOException, InterruptedException {

        final List<Path> tempFiles = new ArrayList<>();

        final BufferedReader reader =
                new BufferedReader(
                        new InputStreamReader(Files.newInputStream(input.toPath())));

        int nLinesPerFile = getBlockSizeEstimation(input.length());
        List<Future<Path>> sortingTasks = new ArrayList<>();
        String line = "";
        int lineCount = 0;
        while (line != null) {
            List<String> lineList = new ArrayList<>(nLinesPerFile);
            while( (lineCount < nLinesPerFile) && (line = reader.readLine()) != null) {
                //remove empty lines. should we keep them?
                if (line.trim().isEmpty())
                    continue;
                lineList.add(line);
                lineCount += 1;
            }
            final Future<Path> task = pool.submit(new Task(lineList));
            sortingTasks.add(task);
            lineCount = 0;
        }

        System.out.println("Splitting and sorting finished. Waiting for background tasks");
        sortingTasks.forEach(t -> {
            try {
                tempFiles.add(t.get());
            } catch (InterruptedException|ExecutionException e) {
                System.out.println("Failed to process task = " + e.toString());
            }
        });
        System.out.println("All background tasks finished");

        Path outFile = Paths.get(input.getName() + ".sorted");
        mergeSortFiles(tempFiles, outFile);

        System.out.println("Sort complete");
    }

    private void mergeSortFiles(List<Path> tempFiles, Path outFile) {

        Comparator<String> cmp = (o1, o2) -> {
           if(o1 == null && o2 == null) return 0;
           if(o1 == null) {
               return -1;
           } else if( o2 == null) {
               return 1;
           } else {
               return o1.compareTo(o2);
           }
        };
        //let the PriorityQueue perform the sorting while reading from the temp files
        PriorityQueue<PeekableBufferedReader> pq =
                new PriorityQueue<>(tempFiles.size(), new Comparator<PeekableBufferedReader>() {
            @Override
            public int compare(PeekableBufferedReader o1, PeekableBufferedReader o2) {
                    String left = o1.peek();
                    String right = o2.peek();
                    return cmp.compare(left, right);
            }
        });

        tempFiles.forEach(file -> {
            try {
                pq.add(new PeekableBufferedReader(
                    new BufferedReader(
                        new InputStreamReader(Files.newInputStream(file)))));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        try (BufferedWriter writer = Files.newBufferedWriter(outFile)) {
            while (pq.size() > 0)
             {
                 PeekableBufferedReader take = pq.remove();
                 if(take == null) {
                     continue;
                 }
                 String line = take.pop();
                 if(line == null) {
                     take.close();
                 } else {
                     writer.write(line);
                     writer.newLine();
                     pq.add(take);
                 }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (Path temp : tempFiles) {
            try {
                Files.delete(temp.toAbsolutePath());
            } catch (IOException e) {
                temp.toFile().deleteOnExit();
            }
        }
    }

    private int getBlockSizeEstimation(long sizeOffFile) {
        //we could try to estimate the number of lines per sort block that would better make use of memory,
        //and the best way to split them among files
        //this number i got it from doing a few tests on my machine.
        // it generated around 51 files and used around 1GB for an example file generated as specified in the test
        //(5_000_000 lines, and close to 770MB). Sorting takes around 11 secs in my machine (4 cores, 8 thread, 8GB)
        return 100_000;
    }

    final class Task implements Callable<Path> {

        private List<String> lines;

        public Task(List<String> lines) {
            this.lines = lines;
        }

        @Override
        public Path call() throws Exception {
            Collections.sort(lines, (a, b) -> a.compareTo(b));
            Path outFilePath = Files.createTempFile("linesorter_", Long.toString(fileIndex.getAndAdd(1)));
            try (BufferedWriter bufferedWriter = Files.newBufferedWriter(outFilePath)) {
                for(String l : lines) {
                    bufferedWriter.write(l);
                    bufferedWriter.newLine();
                }
                bufferedWriter.close();
            }
            lines.clear();
            return outFilePath;
        }

    }
}




