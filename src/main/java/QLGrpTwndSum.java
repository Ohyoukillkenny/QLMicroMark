import streamql.QL;
import streamql.algo.Algo;
import streamql.query.Q;

public class QLGrpTwndSum {

    public static void main(String[] args) {
        long N = Long.parseLong(args[args.length-1]);
        Q<Long,Long> query = QL.groupBy(
                v -> v % 100,
                QL.tWindow(100, QL.reduce(0L,(r,x)->r+x))
        );
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
        System.out.println("QL GrpTwndSum last = " + sink.getLast().toString());
        long timeNano = end - start;
        // throughput: # items per second
        long throughput = (N * 1000L * 1000 * 1000) / timeNano;
        System.out.println(throughput);
    }

}
