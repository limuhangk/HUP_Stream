package ca.pfv.spmf.test;


import ca.pfv.spmf.algorithms.frequentpatterns.HUP_Stream.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;

public class MainTestHUP_Stream {
    public static void main(String[] args) throws IOException {

        String input = fileToPath("dataset/Connect.txt");
        String output = ".//output1236.txt";
        int minutil = 8000000;
        System.out.println("minutil: " + minutil);

        // Win size is the number of batches in a window
        int win_size = 3;

        // number_of_transactions_batch is the number of transactions in a batch
        int batch_size = 10000;

        // Run the algorithm
        AlgoHUP_Stream algorithm = new AlgoHUP_Stream();

        algorithm.runAlgorithm(
                input,
                minutil,
                win_size,
                batch_size,
                output);

        algorithm.printStats();
    }

    public static String fileToPath(String filename) throws UnsupportedEncodingException {
        URL url = MainTestHUP_Stream.class.getResource(filename);
        return java.net.URLDecoder.decode(url.getPath(), "UTF-8");
    }
}
