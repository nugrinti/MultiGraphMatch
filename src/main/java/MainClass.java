import bitmatrix.models.TargetBitmatrix;
import condition.QueryConditionType;
import configuration.Configuration;
import cypher.controller.WhereConditionExtraction;
import cypher.models.QueryCondition;
import cypher.models.QueryStructure;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import out.OutElaborationFiles;
import matching.controllers.MatchingBase;
import matching.controllers.MatchingSimple;
import matching.controllers.MatchingWhere;
import matching.models.OutData;
import reading.FileManager;
import target_graph.graph.TargetGraph;
import tech.tablesaw.api.Table;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

public class MainClass {
   public static void main(String[] args) throws IOException {
       // CONFIGURATION
       Configuration configuration = new Configuration(args);

       // If user passed -r (result_file) but not -o (out_file), use result_file as fallback
       if (configuration.out_file == null && configuration.result_file != null) {
           configuration.out_file = configuration.result_file;
       }

       // PATH
       System.out.println("Reading target graph...");

       Table[] nodesTables = FileManager.files_reading(configuration.nodes_main_directory, ',');
       Table[] edgesTables = FileManager.files_reading(configuration.edges_main_directory, ',');

    // TARGET GRAPH
    OutElaborationFiles outElab = new OutElaborationFiles();
    TargetGraph targetGraph = new TargetGraph(nodesTables, edgesTables, "id", "labels", outElab);

       // QUERIES READING
       List<String> queries = FileManager.query_reading(configuration);

       // prepare collection for successful times
       final java.util.concurrent.CopyOnWriteArrayList<Double> successfulTimes = new java.util.concurrent.CopyOnWriteArrayList<>();

       // Global watchdog: force exit if whole run exceeds configured timeout
       final ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor(r -> {
           Thread t = new Thread(r);
           t.setDaemon(true);
           t.setName("global-timeout-watchdog");
           return t;
       });
       final ScheduledFuture<?> watchdogHandle = watchdog.schedule(() -> {
           int executedSoFar = successfulTimes.size();
           double meanSoFar = 0d;
           if (executedSoFar > 0) meanSoFar = successfulTimes.stream().mapToDouble(Double::doubleValue).average().orElse(0d);
           System.err.println("Global timeout (" + configuration.timeout + "s) reached. Forcing JVM exit.");
           System.err.println("SUMMARY (partial) - Successful queries so far: " + executedSoFar + "\tMean time (s): " + meanSoFar);
           System.exit(124);
       }, configuration.timeout, TimeUnit.SECONDS);
       final Duration tout = Duration.ofSeconds(configuration.timeout);

    // create shared pool (choose desired concurrency)
       int threads = Runtime.getRuntime().availableProcessors();
       ExecutorService queryPool = Executors.newFixedThreadPool(threads);
       CompletionService<QueryResult> cs = new ExecutorCompletionService<>(queryPool);

    // submit tasks
       for (String q : queries) {
           cs.submit(() -> {
               System.out.println(q);
               double totalTime;
               long numOccurrences;

               WhereConditionExtraction where_managing = new WhereConditionExtraction();
               where_managing.where_condition_extraction(q);

               if (where_managing.getWhere_string() != null) { // There are WHERE CONDITIONS
                   where_managing.normal_form_computing();
                   where_managing.buildSetWhereConditions();

                   Int2ObjectOpenHashMap<ObjectArrayList<QueryCondition>> mapOrPropositionToConditionSet = where_managing.getMapOrPropositionToConditionSet();

                   if (mapOrPropositionToConditionSet.size() > 0) { // Multi-Thread (at least one OR)
                       ObjectArrayList<Object2ObjectOpenHashMap<String, Integer>> sharedMemory = new ObjectArrayList<>();
                       double time = System.currentTimeMillis();

                       QueryStructure query_t = new QueryStructure(targetGraph);
                       query_t.parser(q, targetGraph.getNodesLabelsManager(), targetGraph.getEdgesLabelsManager(), nodesTables, edgesTables, Optional.of(where_managing));

                       for (int orIndex = 0; orIndex < mapOrPropositionToConditionSet.size(); orIndex++) {
                           query_t.clean();

                           ObjectArrayList<QueryCondition> simpleConditions = new ObjectArrayList<>();
                           ObjectArrayList<QueryCondition> complexConditions = new ObjectArrayList<>();

                           for (QueryCondition condition : mapOrPropositionToConditionSet.get(orIndex)) {
                               if (condition.getType() == QueryConditionType.SIMPLE) {
                                   simpleConditions.add(condition);
                               } else {
                                   complexConditions.add(condition);
                               }
                           }

                           OutData outData = new OutData();
                           MatchingBase matchingMachine;
                           if (complexConditions.size() == 0) { // No complex conditions
                               matchingMachine = new MatchingSimple(outData, query_t, false, false, Long.MAX_VALUE, targetGraph, targetGraph.getTargetBitmatrix(), simpleConditions, null);
                           } else { // Complex conditions
                               matchingMachine = new MatchingWhere(outData, query_t, false, false, Long.MAX_VALUE, targetGraph, targetGraph.getTargetBitmatrix(), simpleConditions, complexConditions);
                           }
                           matchingMachine.matching();
                           sharedMemory.add(outData.occurrences);
                       }

                       // Union of the occurrences sets
                       ObjectArraySet<String> finalOccurrences = new ObjectArraySet<>();
                       for (Object2ObjectOpenHashMap<String, Integer> occurrences : sharedMemory) {
                           for (String k : occurrences.keySet()) finalOccurrences.add(k);
                       }

                       time = (System.currentTimeMillis() - time) / 1000;
                       totalTime = time;
                       numOccurrences = finalOccurrences.size();
                       System.out.println("FINAL NUMBER OF OCCURRENCES: " + numOccurrences + "\tTIME: " + totalTime);
                   } else { // Single-Thread (only AND)
                       QueryStructure query_t = new QueryStructure(targetGraph);
                       query_t.parser(q, targetGraph.getNodesLabelsManager(), targetGraph.getEdgesLabelsManager(), nodesTables, edgesTables, Optional.of(where_managing));

                       int orIndex = 0;

                       ObjectArrayList<QueryCondition> simpleConditions = new ObjectArrayList<>();
                       ObjectArrayList<QueryCondition> complexConditions = new ObjectArrayList<>();

                       for (QueryCondition condition : mapOrPropositionToConditionSet.get(orIndex)) {
                           if (condition.getType() == QueryConditionType.SIMPLE) {
                               simpleConditions.add(condition);
                           } else {
                               complexConditions.add(condition);
                           }
                       }

                       OutData outData = new OutData();
                       MatchingBase matchingMachine;
                       if (complexConditions.size() == 0) { // No complex conditions
                           matchingMachine = new MatchingSimple(outData, query_t, true, false, Long.MAX_VALUE, targetGraph, targetGraph.getTargetBitmatrix(), simpleConditions, null);
                       } else { // Complex conditions
                           matchingMachine = new MatchingWhere(outData, query_t, true, false, Long.MAX_VALUE, targetGraph, targetGraph.getTargetBitmatrix(), simpleConditions, complexConditions);
                       }

                       outData = matchingMachine.matching();

                       totalTime = outData.getTotalTime();
                       numOccurrences = outData.num_occurrences;
                       System.out.println("FINAL NUMBER OF OCCURRENCES: " + numOccurrences + "\tTIME: " + totalTime);
                   }
               } else { // No WHERE CONDITIONS
                   QueryStructure query = new QueryStructure(targetGraph);
                   query.parser(q, targetGraph.getNodesLabelsManager(), targetGraph.getEdgesLabelsManager(), nodesTables, edgesTables, Optional.empty());

                   OutData outData = new OutData();
                   MatchingSimple matchingMachine = new MatchingSimple(outData, query, true, false, Long.MAX_VALUE, targetGraph, targetGraph.getTargetBitmatrix(), new ObjectArrayList<>(), null);
                   outData = matchingMachine.matching();

                   totalTime = outData.getTotalTime();
                   numOccurrences = outData.num_occurrences;
                   System.out.println("FINAL NUMBER OF OCCURRENCES: " + numOccurrences + "\tTIME: " + totalTime);
               }

               // SAVING
               if (configuration.out_file != null) {
                   System.out.println("Saving result for query: " + q);
                   try {
                       FileManager.saveToCSV(q, configuration.out_file, totalTime, numOccurrences);
                       System.out.println("Saved result for query: " + q + " -> occurrences=" + numOccurrences + " time=" + totalTime);
                   } catch (IOException e) {
                       System.err.println("Failed to save result for query: " + q + " -> " + e.getMessage());
                       e.printStackTrace();
                   }
               }
               return new QueryResult(totalTime, true, q, numOccurrences);
           });
       }

    // collect with per-task timeout (optional)
       for (int i = 0; i < queries.size(); i++) {
           Future<QueryResult> f;
           try {
               f = cs.take(); // waits for the next completed task
           } catch (InterruptedException ie) {
               Thread.currentThread().interrupt();
               break;
           }

           try {
               QueryResult r = f.get(0, TimeUnit.SECONDS);
               if (r != null && r.success) {
                   successfulTimes.add(r.elapsed);
               }
           } catch (TimeoutException | ExecutionException e) {
               // handle
               f.cancel(true);
           } catch (InterruptedException ie) {
               Thread.currentThread().interrupt();
               f.cancel(true);
               break;
           }
       }

    // shutdown pool when done
       queryPool.shutdownNow();
       try {
           if (!queryPool.awaitTermination(5, TimeUnit.SECONDS)) {
               // give it a bit more time
               queryPool.awaitTermination(5, TimeUnit.SECONDS);
           }
       } catch (InterruptedException ie) {
           Thread.currentThread().interrupt();
       }


    // Summary: number of successfully executed queries and mean time per query
       int executedQueries = successfulTimes.size();
       double meanTime = 0d;
       if (executedQueries > 0) {
           meanTime = successfulTimes.stream().mapToDouble(Double::doubleValue).average().orElse(0d);
       }
       System.out.println("SUMMARY - Successful queries: " + executedQueries + "\tMean time (s): " + meanTime);

       // Cancel watchdog if we're finishing before the global timeout
       try {
           watchdogHandle.cancel(false);
           watchdog.shutdownNow();
       } catch (Exception ignored) {}

       System.exit(0);
   }
}

class QueryResult {
   double elapsed;
   boolean success;
   String query;
   long occurrences;

   public QueryResult(double elapsed, boolean success, String query, long occurrences) {
       this.elapsed = elapsed;
       this.success = success;
       this.query = query;
       this.occurrences = occurrences;
   }
}
