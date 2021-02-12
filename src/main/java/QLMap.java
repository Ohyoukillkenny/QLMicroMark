import streamql.QL;
import streamql.algo.Algo;
import streamql.query.Q;

public class QLMap {
    public static void main(String[] args) {
        long N = Long.parseLong(args[args.length-1]);
        Q<Long,Long> query = QL.map(x -> x);
        QLLastSaver<Long> sink = new QLLastSaver<>();
        Algo<Long,Long> algo = query.eval(sink);
        algo.init();

        long start = System.nanoTime();
        for (long i = 0; i < N; i++) {
            algo.next(i);
        }
        algo.end();
        long end = System.nanoTime();

        // force compiler to generate code that processes each item
        System.out.println("QL Map last = " + sink.getLast().toString());
        long timeNano = end - start;
        // throughput: # items per second
        long throughput = (N * 1000L * 1000 * 1000) / timeNano;
        System.out.println(throughput);
    }
}
