import java.io.*;
import java.util.*;

public class TransactionManager {
   String inputFile = "C:/Users/KM. DISHA/command.txt";
   static boolean isComplete = false;
   int ageOfLastTrans = -1;
   boolean DEBUG = false;
   Integer [] allVariables;   // Value : + mean free, - mean locked
   int NOS = 10+1;
   final int NOT = 20+1;
   final int NOV = 20+1;
   Hashtable <String, Transaction> activeTransactions;
   boolean [] [] variableDefinedAtSite;
   public Integer ticker = 0;
   public Site [] sites;
   Thread [] dataManager;
   Thread [] lockManager;

    void debugMsg (String msg1) {
       if (DEBUG) System.out.println (msg1);
    }

    void debugMsg (String msg1, String msg2) {
       if (DEBUG) System.out.println (msg1 +" : "+ msg2);
    }

    void dispMsg (String msg1) {
       System.out.println (msg1);
    }

    void dispMsg (String msg1, String msg2) {
       System.out.println (msg1 +" : "+ msg2);
    }

    public void advanceTicker () {
       synchronized (ticker) {
          ticker = ticker + 1;
          debugMsg ("ticker = " +ticker);
       }
    }

    public TransactionManager (String inputFile, boolean debug, int nos) {
       this.inputFile = inputFile;
       DEBUG = debug;
       NOS = nos;
       allVariables = new Integer [NOV];
       for (int i = 1; i < NOV; i++) allVariables [i] = i;   // Value : + mean free, - mean locked
       ticker = 0;
       activeTransactions = new Hashtable <String, Transaction>();
       sites = new Site [NOS];
       dataManager = new Thread [NOS];
       lockManager = new Thread [NOT];
       variableDefinedAtSite = new boolean [NOV] [NOS];
       for (int s1=1; s1<NOS; s1++) {
          Site site1 = new Site (s1, this);
          dataManager [s1] = new Thread (site1);
          dataManager [s1].setDaemon (true);
          dataManager [s1].start();
          while ( ! dataManager [s1].isAlive () ) {
             sleep (1, "X", "While waiting for Site" +s1+ " to Start Runnung");
          }
          sites [s1] = site1;
       }
         sleep (1000, "Waiting for ALL Sites to Start Runnung",
              "While Waiting for ALL Sites to Start Runnung");
       for (int v1=2; v1<NOV; v1++) {   // Even Indexed Variables
          System.out.print ("Even Variable Index = " +v1);
          System.out.print (" at Sites = ");
          for (int s1=1; s1<NOS; s1++) {
              variableDefinedAtSite [v1] [s1] = true;   // Even Indexed Variables at ALL Sites
              System.out.print (s1 +" ");
          }
          System.out.println ();
          v1++;   // Even Indexed Variables
       }
       for (int v1=1; v1<NOV; v1++) {   // Odd Indexed Variables
          System.out.print ("Odd Variable Index = " +v1);
          int site1;
          site1 = (v1 % 10) + 1;
          System.out.print (" at Site = ");
          for (int s1=1; s1<NOS; s1++) {
             if (s1 == site1) {   // Odd Indexed Variables at ONLY ONE Selected Site
                variableDefinedAtSite [v1] [s1] = true;
                System.out.print (s1 +" ");
                sites [site1].hasUniqueVar = true;
             } else {
                variableDefinedAtSite [v1] [s1] = false;
             }
          }
          System.out.println ();
          v1++;   // Odd Indexed Variables
       }
    }

   void sleep (int ms, String remark1, String remark2) {
      try {
         if (DEBUG && ! remark1.equals ("X"))
            System.out.println (remark1);
         Thread.sleep (2000);
      } catch (InterruptedException ie) {
         System.out.println (remark2 +ie);
      }
   }

   public void startParser() {
      System.out.println ("Input File is : " +inputFile);
      BufferedReader bf = new BufferedReader (new InputStreamReader (System.in));
      try {
         BufferedReader operationReader = new BufferedReader (new FileReader (new File (inputFile)));
         String temp, operation;
         while ((temp = operationReader.readLine()) != null) {
            if (temp.length() == 0) continue;
            if (temp.substring (0,2).equals ("//")) {
               System.out.println (temp);
               continue;
            }
            System.out.println ("\n* * * * * * * * * * * * * * * * * * * * * * * * Input Line : " +temp);
            advanceTicker ();
            temp = temp.toLowerCase();
            temp = temp.replaceAll ("\\s","");
            while (temp.indexOf (";") != -1) {
               parseOperations (temp.substring (0, temp.indexOf (";")));
               temp = temp.substring (temp.indexOf (";") +1);
            }
            if (lockManager [3] != null) debugMsg ("t3", "Trans State = " +lockManager [3].getState());
            parseOperations (temp);
         }
         System.out.println ("End-Of-File");
         sleep (10, "X", "After End-Of_File");
         for (int i=1; i< NOT; i++) {
            if (lockManager [i] != null)
               debugMsg ("t" +i, "Trans State at End = " +lockManager [i].getState());
         }
     } catch (IOException e) {
        e.printStackTrace();
     }
   }

   public void putTransOperation (Transaction transaction1, String operation1) {
	   System.out.println("this operation will be added to buffer now"+operation1+" and transaction is "+ transaction1.transactionId);
      String tId = transaction1.transactionId;
      int tNum = Integer.parseInt (tId.substring (1));
      if (lockManager [tNum] != null)
         dispMsg ("LM State of " +tId+ " Before PutOprsn = " +lockManager [tNum].getState());
      synchronized (transaction1.inputBuffer) {
         transaction1.inputBuffer.add (operation1);
         transaction1.inputBuffer.notify();   // notifyAll NOT required as only one transaction will wait for its own input buffer
      }
      sleep (1000, "X", "While Waiting for transaction for one operation : ");
      if (lockManager [tNum] != null)
         dispMsg ("LM State of " +tId+ " After PutOprsn = " +lockManager [tNum].getState());
   }

   public void putSiteOperation (Site site1, String operation1) {
      int sNum = site1.siteID;
      if (dataManager [sNum] != null)
         debugMsg ("DM State of Site" +sNum+ " Before PutOprsn = " +dataManager [sNum].getState());
      synchronized (site1.inputBuffer) {
         site1.inputBuffer.add (operation1);
         site1.inputBuffer.notify();   // notifyAll NOT required as only one Site will wait for its own input buffer
      }
      sleep (1000, "X", "While Waiting for site for one operation : ");
      if (dataManager [sNum] != null)
         debugMsg ("DM State of Site" +sNum+ " After PutOprsn = " +dataManager [sNum].getState());
   }

   public void parseOperations (String transOperation) {
      int transactionNum;
      String transactionId;
      String temp = new String (transOperation);
      if (temp.indexOf ("(") != -1) {
         String operation = temp.substring (0, temp.indexOf ("("));
         temp = temp.substring (temp.indexOf ("(") +1);
         if (operation.equals ("begin")) {
            transactionId = temp.substring (0, temp.indexOf (")"));
            transactionNum = Integer.parseInt (transactionId.substring(1));
            if (transactionNum < 1 || transactionNum > NOT) {
               dispMsg ("Transaction Num Out Of Range !");
               return;
            }
            if (activeTransactions.contains (transactionId)) {
               System.out.println ("Transaction " +transactionId+ " Already Exists !");
            } else {
               System.out.println ("Creating Transaction " +transactionId);
               Transaction transaction1 = new Transaction (transactionId, false, this);
               lockManager [transactionNum] = new Thread (transaction1);
               lockManager [transactionNum].setDaemon (true);
               lockManager [transactionNum].start();
               while ( ! lockManager [transactionNum].isAlive () ) {
                  sleep (1000, "Waiting for Transaction " +transactionId+ " to Start",
                       "While Waiting for Transaction " +transactionId+ " to Start");
               }
               activeTransactions.put (transactionId, transaction1);
            }
         } else if (operation.equals ("beginro")) {
            transactionId = temp.substring (0, temp.indexOf (")"));
            transactionNum = Integer.parseInt (transactionId.substring(1));
            if (transactionNum < 1 || transactionNum > NOT) {
               dispMsg ("Transaction Num Out Of Range !");
               return;
            }
            if (activeTransactions.contains (transactionId)) {
               System.out.println ("Transaction " +transactionId+ " Already Exists !");
            } else {
               System.out.println ("Creating Read Only Transaction " +transactionId);
               Transaction transaction1 = new Transaction (transactionId, true, this);
               lockManager [transactionNum] = new Thread (transaction1);
               lockManager [transactionNum].setDaemon (true);
               lockManager [transactionNum].start();
               while ( ! lockManager [transactionNum].isAlive () ) {
                  sleep (1000, "Waiting for RO Transaction", "While waiting for RO Transaction " +transactionId+ " to Start");
               }
               activeTransactions.put (transactionId, transaction1);
               if (lockManager [transactionNum] != null) dispMsg (transactionId, "LM State 1 = " +lockManager [transactionNum].getState());
               sleep (1000, "Creating RO Transaction", "While Creating RO Transaction " +transactionId);
               if (lockManager [transactionNum] != null) dispMsg (transactionId, "LM State 2 = " +lockManager [transactionNum].getState());
            }
         } else if (operation.equals ("r")) {
            transactionId = temp.substring (0, temp.indexOf (","));
            temp = temp.substring (temp.indexOf (",") +1);
            int variableIndex = Integer.parseInt (temp.substring (1, temp.indexOf (")")));
            if (variableIndex < 1 || variableIndex > NOV) {
               dispMsg ("Variable Index Out Of Range !");
               return;
            }
            Transaction transaction1 = activeTransactions.get (transactionId);
            if (transaction1 == null) {
               System.out.println ("No Such Transaction " +transactionId);
            } else {
               putTransOperation (transaction1, transOperation);
            }
            while(Transaction.isProcess){}
            
         } else if (operation.equals ("w")) {
            boolean operationSuccess = false;
            transactionId = temp.substring (0, temp.indexOf (","));
            transactionNum = Integer.parseInt (transactionId.substring(1));
            temp = temp.substring (temp.indexOf (",") +1);
            int variableIndex = Integer.parseInt (temp.substring (1, temp.indexOf (",")));
            if (variableIndex < 1 || variableIndex > NOV) {
               dispMsg ("Variable Index Out Of Range !");
               return;
            }
            Transaction transaction1 = activeTransactions.get (transactionId);
            if (transaction1 == null) {
               System.out.println ("No Such Transaction " +transactionId);
            } else {
               putTransOperation (transaction1, transOperation);
               sleep (1000, "Waiting for Site to write in variable",
                           "While Waiting for Site to write in variable : ");
            }
            while(Transaction.isProcess){}

         } else if (operation.equals ("end")) {
            transactionId = temp.substring (0, temp.indexOf (")"));
            transactionNum = Integer.parseInt (transactionId.substring(1));
            Transaction transaction1 = activeTransactions.get (transactionId);
            if (transaction1 == null) {
               System.out.println ("No Such Transaction " +transactionId);
            } else {
               dispMsg ("added operation " +transOperation+ " in " +transactionId);
               putTransOperation (transaction1, transOperation);
               
               sleep (1000, "Actually Ending Transaction", "Before Actually Ending Transaction");
            }
            while(Site.isRunning){}
         }
         else if (operation.equals ("fail")) {
        	int site1 = Integer.parseInt (temp.substring (0, temp.indexOf (")")));
            if (site1 < 1 || site1 > NOS) {
               dispMsg ("Site Num Out Of Range !");
               return;
            }
            putSiteOperation (sites [site1], transOperation);
            sleep (1000, "Waiting for Site to fail", "While Waiting for Site to fail");
            if (true) {
               Enumeration <String> activeTrans = activeTransactions.keys();
               while (activeTrans.hasMoreElements()) {
            	   
                  String transId1 = (String) activeTrans.nextElement();
                  System.out.println(transId1);
                  Transaction trans1 = activeTransactions.get (transId1);
                  Enumeration <Integer> writtenSites = trans1.sitesWritten.keys();
                  Enumeration <Integer> readSites = trans1.sitesRead.keys();
                  while (writtenSites.hasMoreElements()) {
                     int writtenSite = (Integer) writtenSites.nextElement();
                     if (writtenSite == site1) {
                        debugMsg ("As Site" +site1+ " has Failed, and it has a Unique Variable, its Transactions Should Abort");
                        putTransOperation (trans1, "ABORT(" +transId1+ ")");
                        sleep (500, "Waiting for Transactions to Abort", "While Waiting for Transactions to Abort");
                     }
                  }
                  while (readSites.hasMoreElements()) {
                      int readSite = (Integer) readSites.nextElement();
                      if (readSite == site1) {
                         debugMsg ("As Site" +site1+ " has Failed, and it has a Unique Variable, its Transactions Should Abort");
                         putTransOperation (trans1, "ABORT(" +transId1+ ")");
                         sleep (500, "Waiting for Transactions to Abort", "While Waiting for Transactions to Abort");
                      }
                   }
               }
            }
            //while(Transaction.isProcess){}
            while(Site.isRunning){}
         } else if (operation.equals ("recover")) {
            int site1 = Integer.parseInt (temp.substring (0, temp.indexOf (")")));
            if (site1 < 1 || site1 > NOS) {
               dispMsg ("Site Num Out Of Range !");
               return;
            }
            putSiteOperation (sites [site1], "recover()");
            //while(Transaction.isProcess){}
            while(Site.isRunning){}

         } else if (operation.equals ("dump")) {     // dump ALL Variables from ALL Sites
            if (transOperation.equals ("dump()")) {
               for (int s1=1; s1<NOS; s1++) {
                  putSiteOperation (sites [s1], "dumpAllItems()");
               }
               sleep (1000, "* * * * * * * * * * * TM Waiting for Sites to display ALL variables * * * * * * *",
                            "While Waiting for Sites to display ALL variables : ");
            } else if (temp.indexOf ("x") != -1) {   // dump THE Variable from ALL Sites
               boolean varDefined = false;
               int variableIndex = Integer.parseInt (temp.substring (1, temp.indexOf (")")));
               if (variableIndex < 1 || variableIndex > NOV) {
                  dispMsg ("Variable Index Out Of Range !");
                  return;
               }
               for (int s1=1; s1<NOS; s1++) {
                  if (variableDefinedAtSite [variableIndex] [s1]) {
                     putSiteOperation (sites[s1], "dumpSingleItem(" +variableIndex+ ")");
                     varDefined = true; // may be shifted to Class Site
                  }
               }
               sleep (1000, "* * * * * * * * * * * TM Waiting for Sites to display variable * * * * * * *",
                           "While Waiting for Sites to display variable : ");
               if ( ! varDefined ) System.out.println ("Variable X" +variableIndex+ " NOT Defined at ANY Site !");
            } else {                                 // dump ALL Variables from THE Site
               int site1 = Integer.parseInt (temp.substring (0, temp.indexOf (")")));
               if (site1 < 1 || site1 > NOS) {
                  dispMsg ("Site Num Out Of Range !");
                  return;
               }
               for (int v1=1; v1<NOV; v1++) {
                  if (variableDefinedAtSite [v1] [site1]) {
                     putSiteOperation (sites [site1], "dumpSingleItem(" +v1+ ")");
                  }
               }
               sleep (1000, "* * * * * * * * * * * TM Waiting for Sites to display variable * * * * * * *",
                           "While Waiting for Sites to display variable : ");
            }
            while(Site.isRunning){}
         } else if (operation.equals ("sleep")) {     // sleep for testing
            int ms = Integer.parseInt (temp.substring (0, temp.indexOf (")")));
            sleep (ms, "Sleeping for Testing", "While Sleeping for Testing");
         } else {
            System.out.println ("Comment OR Invalid operation !");
         }
      } else {
         System.out.println ("Comment OR Syntax Error");
      }
   }
}