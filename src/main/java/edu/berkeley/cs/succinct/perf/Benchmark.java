package edu.berkeley.cs.succinct.perf;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.perf.buffers.SuccinctBufferBench;
import edu.berkeley.cs.succinct.perf.buffers.SuccinctFileBufferBench;
import edu.berkeley.cs.succinct.perf.buffers.TFSSuccinctFileBufferBench;
import edu.berkeley.cs.succinct.perf.streams.SuccinctFileStreamBench;
import edu.berkeley.cs.succinct.perf.streams.SuccinctStreamBench;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

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
        options.addOption("t", true, "Tachyon master path (REQUIRED if file on TFS)");
        options.addOption("n", true, "Number of threads for throughput bench (on SuccinctFileBuffer[-TFS])");
        options.addOption("e", true, "Length of extract queries");

        HelpFormatter formatter = new HelpFormatter();

        try {
            // Parse the command line options
            CommandLine line = parser.parse(options, args);

            String benchType = line.getOptionValue("b");
            String resPath = line.getOptionValue("r");
            String queryFile = line.getOptionValue("q");
            String storageModeString = line.getOptionValue("s");
            String dataPath = line.getOptionValue("d");
            String tfsPath = line.getOptionValue("t");
            int threads = 1, extrLen = 1000;

            if (line.getOptionValue("n") != null) {
                threads = Integer.parseInt(line.getOptionValue("n"));
            }

            if (line.getOptionValue("e") != null) {
                extrLen = Integer.parseInt(line.getOptionValue("e"));
            }

            StorageMode storageMode;
            if(storageModeString == null || storageModeString.equals("MEMORY_ONLY")) {
                storageMode = StorageMode.MEMORY_ONLY;
            } else {
                storageMode = StorageMode.MEMORY_MAPPED;
            }

            if(dataPath == null) {
                System.out.println("Data path must be specified.");
                formatter.printHelp("succinct-perf", options);
                System.exit(0);
            }
            else {
                System.out.println("Benching on file " + dataPath);
            }

            if(resPath == null) {
                System.out.println("Result path not specified; results will be stored in results/");
                resPath = "results/";
                File resDir = new File(resPath);
                if(!resDir.exists())
                    resDir.mkdir();
                resPath += "res";
            }

            if(benchType == null) {
                benchType = "all";
            }

            String[] benchParams = benchType.split("\\.");
            System.out.println("Benchmark Parameters (" + benchParams.length +  "): ");
            for(String param : benchParams) {
                System.out.print(" \"" + param + "\" ");
            }
            System.out.println();
            if(benchParams.length == 1) {
                if (benchParams[0].equals("all")) {
                    System.out.println("Benchmarking all classes and methods...");
                    new SuccinctBufferBench(dataPath, storageMode).benchAll(resPath + "_sb");
                    new SuccinctFileBufferBench(dataPath, storageMode)
                        .benchAll(queryFile, resPath + "_sfb", threads, extrLen);
                    new SuccinctStreamBench(dataPath).benchAll(resPath + "_ss");
                    new SuccinctFileStreamBench(dataPath).benchAll(queryFile, resPath + "_sfs");
                } else if(benchParams[0].equals("SuccinctBuffer")) {
                    System.out.println("Benchmarking all methods for SuccinctBuffer...");
                    new SuccinctBufferBench(dataPath, storageMode).benchAll(resPath);
                } else if(benchParams[0].equals("SuccinctBuffer-TFS")) {
                    System.out.println("Benchmarking all methods for SuccinctBuffer... (from TFS)");
                    new SuccinctBufferBench(tfsPath, dataPath).benchAll(resPath);
                } else if(benchParams[0].equals("SuccinctFileBuffer")) {
                    System.out.println("Benchmarking all methods for SuccinctFileBuffer...");
                    new SuccinctFileBufferBench(dataPath, storageMode).benchAll(queryFile, resPath, threads, extrLen);
                } else if(benchParams[0].equals("SuccinctFileBuffer-TFS")) {
                    System.out.println("Benchmarking all methods for SuccinctFileBuffer (from TFS)...");
                    new SuccinctFileBufferBench(tfsPath, dataPath).benchAll(queryFile, resPath, threads, extrLen);
                } else if(benchParams[0].equals("SuccinctStream")) {
                    System.out.println("Benchmarking all methods for SuccinctStream...");
                    new SuccinctStreamBench(dataPath).benchAll(resPath);
                } else if(benchParams[0].equals("SuccinctFileStream")) {
                    System.out.println("Benchmarking all methods for SuccinctFileStream...");
                    new SuccinctFileStreamBench(dataPath).benchAll(queryFile, resPath);
                } else if(benchParams[0].equals("VanillaTachyon")) {
                    System.out.println("Benchmarking all methods for VanillaTachyon...");
                    new VanillaTachyonBench(tfsPath, dataPath).benchAll(resPath, extrLen, threads);
                } else {
                    System.out.println("Invalid benchmark specification.");
                    System.out.println("Test class must be one of SuccinctBuffer, SuccinctFileBuffer, SuccinctFileBuffer-TFS, SuccinctStream, SuccinctFileStream or all");
                    formatter.printHelp("succinct-perf", options);
                    System.exit(0);
                }
            } else if(benchParams.length == 2) {
                if (benchParams[0].equals("SuccinctBuffer")) {
                    if(benchParams[1].equals("lookupNPA")) {
                        System.out.println("Benchmarking SuccinctBuffer.lookupNPA...");
                        new SuccinctBufferBench(dataPath, storageMode).benchLookupNPA(resPath);
                    } else if(benchParams[1].equals("lookupSA")) {
                        System.out.println("Benchmarking SuccinctBuffer.lookupSA...");
                        new SuccinctBufferBench(dataPath, storageMode).benchLookupSA(resPath);
                    } else if(benchParams[1].equals("lookupISA")) {
                        System.out.println("Benchmarking SuccinctBuffer.lookupISA...");
                        new SuccinctBufferBench(dataPath, storageMode).benchLookupISA(resPath);
                    } else {
                        System.out.println("Invalid benchmark specification.");
                        formatter.printHelp("succinct-perf", options);
                        System.exit(0);
                    }
                } else if (benchParams[0].equals("SuccinctBuffer-TFS")) {
                    if(benchParams[1].equals("lookupNPA")) {
                        System.out.println("Benchmarking SuccinctBuffer.lookupNPA (from TFS)...");
                        new SuccinctBufferBench(tfsPath, dataPath).benchLookupNPA(resPath);
                    } else if(benchParams[1].equals("lookupSA")) {
                        System.out.println("Benchmarking SuccinctBuffer.lookupSA (from TFS)...");
                        new SuccinctBufferBench(tfsPath, dataPath).benchLookupSA(resPath);
                    } else if(benchParams[1].equals("lookupISA")) {
                        System.out.println("Benchmarking SuccinctBuffer.lookupISA (from TFS)...");
                        new SuccinctBufferBench(tfsPath, dataPath).benchLookupISA(resPath);
                    } else {
                        System.out.println("Invalid benchmark specification.");
                        formatter.printHelp("succinct-perf", options);
                        System.exit(0);
                    }
                }
                else if(benchParams[0].equals("SuccinctFileBuffer")) {
                    if(benchParams[1].equals("count")) {
                        System.out.println("Benchmarking SuccinctFileBuffer.count...");
                        new SuccinctFileBufferBench(dataPath, storageMode).benchCountLatency(queryFile, resPath);
                    } else if(benchParams[1].equals("search")) {
                        System.out.println("Benchmarking SuccinctFileBuffer.search...");
                        new SuccinctFileBufferBench(dataPath, storageMode).benchSearchLatency(queryFile, resPath);
                    } else if(benchParams[1].equals("extract")) {
                        System.out.println("Benchmarking SuccinctFileBuffer.extract...");
                        new SuccinctFileBufferBench(dataPath, storageMode).benchExtractLatency(resPath, extrLen);
                    } else {
                        System.out.println("Invalid benchmark specification.");
                        formatter.printHelp("succinct-perf", options);
                        System.exit(0);
                    }
                } else if(benchParams[0].equals("SuccinctFileBuffer-TFS")) {
                    if (benchParams[1].equals("count-lat")) {
                        System.out.println("Benchmarking SuccinctFileBuffer.count latency (from TFS)...");
                        new SuccinctFileBufferBench(tfsPath, dataPath).benchCountLatency(queryFile, resPath);
                    } else if (benchParams[1].equals("search-lat")) {
                        System.out.println("Benchmarking SuccinctFileBuffer.search latency (from TFS)...");
                        new SuccinctFileBufferBench(tfsPath, dataPath).benchSearchLatency(queryFile, resPath);
                    } else if (benchParams[1].equals("extract-lat")) {
                        System.out.println("Benchmarking SuccinctFileBuffer.extract latency (from TFS)...");
                        new SuccinctFileBufferBench(tfsPath, dataPath).benchExtractLatency(resPath, extrLen);
                    } else if (benchParams[1].equals("extract-thr")) {
                        System.out.println("Benchmarking SuccinctFileBuffer.extract throughput (from TFS)...");
                        new SuccinctFileBufferBench(tfsPath, dataPath).benchExtractThroughput(extrLen, threads);
                    } else if (benchParams[1].equals("search-thr")) {
                        System.out.println("Benchmarking SuccinctFileBuffer.search throughput (from TFS)...");
                        new SuccinctFileBufferBench(tfsPath, dataPath).benchSearchThroughput(queryFile, threads);
                    } else {
                        System.out.println("Invalid benchmark specification.");
                        formatter.printHelp("succinct-perf", options);
                        System.exit(0);
                    }
                } else if (benchParams[0].equals("VanillaTachyon")) {
                    if (benchParams[1].equals("extr-lat")) {
                        new VanillaTachyonBench(tfsPath, dataPath).benchExtractLatency(resPath, extrLen);
                    }
                    else if (benchParams[1].equals("extr-thr")) {
                        new VanillaTachyonBench(tfsPath, dataPath).benchExtractThroughput(extrLen, threads);
                    }
                } else if(benchParams[0].equals("SuccinctStream")) {
                    if(benchParams[1].equals("lookupNPA")) {
                        System.out.println("Benchmarking SuccinctStream.lookupNPA...");
                        new SuccinctStreamBench(dataPath).benchLookupNPA(resPath);
                    } else if(benchParams[1].equals("lookupSA")) {
                        System.out.println("Benchmarking SuccinctStream.lookupSA...");
                        new SuccinctStreamBench(dataPath).benchLookupSA(resPath);
                    } else if(benchParams[1].equals("lookupISA")) {
                        System.out.println("Benchmarking SuccinctStream.lookupISA...");
                        new SuccinctStreamBench(dataPath).benchLookupISA(resPath);
                    } else {
                        System.out.println("Invalid benchmark specification.");
                        formatter.printHelp("succinct-perf", options);
                        System.exit(0);
                    }
                } else if(benchParams[0].equals("SuccinctFileStream")) {
                    if(benchParams[1].equals("count")) {
                        System.out.println("Benchmarking SuccinctFileStream.count...");
                        new SuccinctFileStreamBench(dataPath).benchCount(queryFile, resPath);
                    } else if(benchParams[1].equals("search")) {
                        System.out.println("Benchmarking SuccinctFileStream.search...");
                        new SuccinctFileStreamBench(dataPath).benchSearch(queryFile, resPath);
                    } else if(benchParams[1].equals("extract")) {
                        System.out.println("Benchmarking SuccinctFileStream.extract...");
                        new SuccinctFileStreamBench(dataPath).benchExtract(resPath);
                    } else {
                        System.out.println("Invalid benchmark specification.");
                        System.out.println("Test method must be one of count, search or extract");
                        formatter.printHelp("succinct-perf", options);
                        System.exit(0);
                    }
                } else {
                    System.out.println("Invalid benchmark specification.");
                    System.out.println("Test class must be one of SuccinctBuffer, SuccinctFileBuffer, SuccinctStream or SuccinctFileStream");
                    formatter.printHelp("succinct-perf", options);
                    System.exit(0);
                }
            } else {
                System.out.println("Invalid benchmark specification; benchParams.length = " + benchParams.length);
                formatter.printHelp("succinct-perf", options);
                System.exit(0);
            }

        } catch (ParseException exception) {
            System.out.println("Could not parse command line options: " + exception.getMessage());
            formatter.printHelp("succinct-perf", options);
            System.exit(0);
        } catch (IOException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }
}
