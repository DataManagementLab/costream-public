package costream.plan.executor.tests;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

public class SSHTest {
    public static void main(String[] args) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder();
        StringBuilder builder = new StringBuilder();

        for (String host : Arrays.asList("nodeB0", "nodeC0", "nodeD0", "nodeE0")){
            processBuilder.command("ssh", host, "tail", "/var/bigdata/storm/logs/nimbus.log");
            //processBuilder.command("ssh", host, "cat", "/var/bigdata/storm/logs/query.placement");
            Process process = processBuilder.start();
            process.waitFor();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

            String line;
            while ( (line = reader.readLine()) != null) {
                builder.append(line);
                builder.append(System.getProperty("line.separator"));
            }
            String result = builder.toString();
            System.out.println(host + result);
        }
    }
}
