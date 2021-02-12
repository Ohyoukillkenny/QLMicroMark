import streamql.QL;
import streamql.algo.Algo;
import streamql.query.Q;

public class QLSwndSum {

    public static void main(String[] args) {
        long N = Long.parseLong(args[args.length-1]);
        Q<Long,Long> query = QL.sWndReduceI(100, 1, 0L, (s,x)->s+x, (s,x)->s-x);
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
        System.out.println("QL SwndSum last = " + sink.getLast().toString());
        long timeNano = end - start;
        // throughput: # items per second
        long throughput = (N * 1000L * 1000 * 1000) / timeNano;
        System.out.println(throughput);
    }

}
