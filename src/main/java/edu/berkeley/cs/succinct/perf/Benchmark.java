package edu.berkeley.cs.succinct.perf;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.perf.buffers.SuccinctBufferBench;
import edu.berkeley.cs.succinct.perf.buffers.SuccinctFileBufferBench;
import edu.berkeley.cs.succinct.perf.streams.SuccinctFileStreamBench;
import edu.berkeley.cs.succinct.perf.streams.SuccinctStreamBench;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;

public class Benchmark {

    // Main class
    public static void main(String[] args) {
        CommandLineParser parser = new DefaultParser();

        Options options = new Options();
        options.addOption("b", true, "The benchmark to run; the specification format is <class-name>.<method-name>." +
            " If only class name is specified, then all benchmarks for that class name will be run." +
            " To run all benchmarks for all classes, specify \"all\"");
        options.addOption("r", true, "Path where the results will be stored.");
        options.addOption("q", true, "Path to query file that contains query strings" +
            " (Required for search/count benchmarks).");
        options.addOption("s", true, "Storage mode for SuccinctBuffer benchmarks."
                + " Can be MEMORY_ONLY or MEMORY_MAPPED.");
        options.addOption("d", true, "Path to serialized Succinct data. (REQUIRED)");

        HelpFormatter formatter = new HelpFormatter();

        try {
            // Parse the command line options
            CommandLine line = parser.parse(options, args);

            String resPath = line.getOptionValue("r");
            String queryFile = line.getOptionValue("q");
            String storageModeString = line.getOptionValue("s");
            StorageMode storageMode = (storageModeString == null || storageModeString == "MEMORY_ONLY") ?
                    StorageMode.MEMORY_ONLY : StorageMode.MEMORY_MAPPED;
            String benchType = line.getOptionValue("b");
            String dataPath = line.getOptionValue("d");

            if(dataPath == null) {
                System.out.println("Data path must be specified.");
                formatter.printHelp("succinct-perf", options);
                System.exit(0);
            }

            if(resPath == null) {
                System.out.println("Result path not specified; results will be stored in results/");
                resPath = "results/";
                File resDir = new File(resPath);
                if(!resDir.exists())
                    resDir.mkdir();
                resPath += "res";
            }

            if(benchType != null) {
                String[] benchParams = benchType.split("\\.");
                if(benchParams.length == 1) {
                    if (benchParams[0] == "all") {
                        new SuccinctBufferBench(dataPath, storageMode).benchAll(resPath);
                        new SuccinctFileBufferBench(dataPath, storageMode).benchAll(queryFile, resPath);
                        new SuccinctStreamBench(dataPath).benchAll(resPath);
                        new SuccinctFileStreamBench(dataPath).benchAll(queryFile, resPath);
                    } else if(benchParams[0] == "SuccinctBuffer") {
                        new SuccinctBufferBench(dataPath, storageMode).benchAll(resPath);
                    } else if(benchParams[0] == "SuccinctFileBuffer") {
                        new SuccinctFileBufferBench(dataPath, storageMode).benchAll(queryFile, resPath);
                    } else if(benchParams[0] == "SuccinctStream") {
                        new SuccinctStreamBench(dataPath).benchAll(resPath);
                    } else if(benchParams[0] == "SuccinctFileStream") {
                        new SuccinctFileStreamBench(dataPath).benchAll(queryFile, resPath);
                    } else {
                        System.out.println("Invalid benchmark specification.");
                        formatter.printHelp("succinct-perf", options);
                        System.exit(0);
                    }
                } else if(benchParams.length == 2) {
                    if (benchParams[0] == "SuccinctBuffer") {
                        if(benchParams[1] == "lookupNPA") {
                            new SuccinctBufferBench(dataPath, storageMode).benchLookupNPA(resPath);
                        } else if(benchParams[1] == "lookupSA") {
                            new SuccinctBufferBench(dataPath, storageMode).benchLookupSA(resPath);
                        } else if(benchParams[2] == "lookupISA") {
                            new SuccinctBufferBench(dataPath, storageMode).benchLookupISA(resPath);
                        } else {
                            System.out.println("Invalid benchmark specification.");
                            formatter.printHelp("succinct-perf", options);
                            System.exit(0);
                        }
                    } else if(benchParams[0] == "SuccinctFileBuffer") {
                        if(benchParams[1] == "count") {
                            new SuccinctFileBufferBench(dataPath, storageMode).benchCount(queryFile, resPath);
                        } else if(benchParams[1] == "search") {
                            new SuccinctFileBufferBench(dataPath, storageMode).benchSearch(queryFile, resPath);
                        } else if(benchParams[1] == "extract") {
                            new SuccinctFileBufferBench(dataPath, storageMode).benchExtract(resPath);
                        } else {
                            System.out.println("Invalid benchmark specification.");
                            formatter.printHelp("succinct-perf", options);
                            System.exit(0);
                        }
                    } else if(benchParams[0] == "SuccinctStream") {
                        if(benchParams[1] == "lookupNPA") {
                            new SuccinctStreamBench(dataPath).benchLookupNPA(resPath);
                        } else if(benchParams[1] == "lookupSA") {
                            new SuccinctStreamBench(dataPath).benchLookupSA(resPath);
                        } else if(benchParams[2] == "lookupISA") {
                            new SuccinctStreamBench(dataPath).benchLookupISA(resPath);
                        } else {
                            System.out.println("Invalid benchmark specification.");
                            formatter.printHelp("succinct-perf", options);
                            System.exit(0);
                        }
                    } else if(benchParams[0] == "SuccinctFileBuffer") {
                        if(benchParams[1] == "count") {
                            new SuccinctFileStreamBench(dataPath).benchCount(queryFile, resPath);
                        } else if(benchParams[1] == "search") {
                            new SuccinctFileStreamBench(dataPath).benchSearch(queryFile, resPath);
                        } else if(benchParams[1] == "extract") {
                            new SuccinctFileStreamBench(dataPath).benchExtract(resPath);
                        } else {
                            System.out.println("Invalid benchmark specification.");
                            formatter.printHelp("succinct-perf", options);
                            System.exit(0);
                        }
                    }
                } else {
                    System.out.println("Invalid benchmark specification.");
                    formatter.printHelp("succinct-perf", options);
                    System.exit(0);
                }
            }
        } catch (ParseException exception) {
            System.out.println("Could not parse command line options: " + exception.getMessage());
            formatter.printHelp("succinct-perf", options);
            System.exit(0);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }
}
