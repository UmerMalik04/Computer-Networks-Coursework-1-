// Coursework 2025/2026
//// Submission by
//  Umer Malik
//  230035630
//  Umer.malik@city.ac.uk
// This is a test program to show how Node.java can be used on the Azure lab.

class AzureLabTest {
    public static void main(String[] args) {
        String emailAddress = "Put your e-mail address here!";
        String ipAddress = "Put the IP address of Azure lab machine here!";

        try {
            Node node = new Node();
            String nodeName = "N:" + emailAddress;
            node.setNodeName(nodeName);

            int port = 20110;
            node.openPort(port);

            System.out.println("Waiting for another node to get in contact");
            node.handleIncomingMessages(12 * 1000);

            System.out.println("Getting the poem...");
            for (int i = 0; i < 7; ++i) {
                String key = "D:jabberwocky" + i;
                String value = node.read(key);
                if (value == null) {
                    System.err.println("Can't find poem verse " + i);
                    System.exit(2);
                } else {
                    System.out.println(value);
                }
            }

            System.out.println("Writing a marker so it's clear my code works");
            {
                String key = "D:" + emailAddress;
                String value = "It works!";
                boolean success = node.write(key, value);
                if (!success) {
                    System.err.println("Write failed");
                }
                System.out.println(node.read(key));
            }

            System.out.println("Letting other nodes know where we are");
            node.write(nodeName, ipAddress + ":" + port);

            System.out.println("Handling incoming connections");
            node.handleIncomingMessages(0);
        } catch (Exception e) {
            System.err.println("Exception during AzureLabTest");
            e.printStackTrace(System.err);
        }
    }
}
