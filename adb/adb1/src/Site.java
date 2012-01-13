import java.io.*;
import java.util.*;

public class Site implements Runnable {
	
   TransactionManager tm;
   String transOperation;
   public ArrayList<String> activeTransactions=new ArrayList<String>();
   public int state = 0;
//                                 0           1           2           3          4           5
   public String [] states = {"Shutdown", "Sleeping", "Starting", "Recovery", "Running", "Shutting Down"};
   int siteID;
   int siteAge;
   Variable variables[];
   Vector <String> inputBuffer;
   boolean hasUniqueVar = false;
   public static boolean isRunning = false;
   public Site (int ID, TransactionManager tm) {
      this.tm = tm;
      siteID = ID;
      siteAge = tm.ticker;
      variables = new Variable [tm.NOV];
      for (int v1=1; v1<tm.NOV; v1++)
          variables [v1] = new Variable (v1);
      inputBuffer = new Vector <String> ();
      state = 2;
      tm.dispMsg ("Site" +siteID, "Registered");
   }

   public boolean isVariableLockedByOther (String transactionId, int variableIndex, boolean isTransactionRO, int value) {
	   
	   System.out.println("lockstate at site level is "+this.variables [variableIndex].lockState);
      Transaction transaction1 = tm.activeTransactions.get (transactionId);
      if (transaction1 == null) {
         tm.dispMsg ("Site" +siteID, " No Such Transaction " +transactionId);
         return true;
      }
      int age = transaction1.ageOfTransaction;
      Variable transVar = variables [variableIndex];
      System.out.println("Locking Transaction Id: "+transVar.isWriteLockedBy);
      String lockingTransactionId = transVar.isWriteLockedBy;
      ArrayList<String> readLockingTransactionId = transVar.isReadLockedBy;
      if (transVar.lockState == 0) {                      // if variable is NOT locked
         transVar.isWriteLockedBy = transactionId;        //    lock the variable
         transVar.lockState = 2;                          //    Write-Lock acquired
         return (false);
      } else if (transVar.lockState == 1 && lockingTransactionId.equals (transactionId)) {               // if variable READ-locked
         transVar.isWriteLockedBy = transactionId;        //    lock the variable
         transVar.lockState = 2;                          //    Write-Lock acquired
         return (false);
      } else if (transVar.lockState == 2
       && lockingTransactionId.equals (transactionId)) { // if THIS transaction has Write-Locked the variable
    	  
         return (false);
      } else {                                                        // some OTHER transaction has write-locked the variable
    	  
    	  System.out.println("this is the locking transaction which is getting null : "+lockingTransactionId );
         if (tm.activeTransactions.get (lockingTransactionId) == null || lockingTransactionId.trim().equals("")) {
        	 if(readLockingTransactionId.size()==0){
            tm.dispMsg ("Site" +siteID, "Error 1 : " +lockingTransactionId);
            return true;
        	 }
        	 else{
        		 for(String s : readLockingTransactionId){
        		 if (tm.activeTransactions.get (s) == null || s.trim().equals("")) {
    				 return false;
    			 }
        		 }
        		 boolean x = false;
        		 int abc = 1;
        		 for(String s : readLockingTransactionId){
        			 System.out.println("counter = " +abc);
        			 abc++;
        		 if (age < tm.activeTransactions.get (s).ageOfTransaction && !s.equals(transactionId)) { 
        			 if (tm.activeTransactions.get (lockingTransactionId) == null || lockingTransactionId.trim().equals("")) {
        				 return false;
        			 }
        			 // THIS transaction is NOT younger
        	            tm.dispMsg ("Site" +siteID, transactionId+ "(" +age+ ") is NOT Younger than locking " +s+ "(" +tm.activeTransactions.get(s).ageOfTransaction+ ")");
        	            String repeatOprsn = "w(" +transactionId+ ",X" +variableIndex+ "," +value+ ")";
        	            transaction1.inputBuffer.insertElementAt (repeatOprsn, 0);
//        	            tm.sleep (100, "Site" +siteID+ " : Waiting for " +transactionId+ " to write X" +variableIndex,
//        	                           "Site" +siteID+ " : Waiting for " +transactionId+ " to write X" +variableIndex);
        	            x = true;                                                                      // Here, the transactionId should go in wait-state
        	         } else if (!s.equals(transactionId)){                                                                               // THIS transaction is younger
        	            tm.dispMsg ("Site" +siteID, transactionId+ "(" +age+ ") is Younger than locking " +s+ "(" +tm.activeTransactions.get(s).ageOfTransaction+ ")");
//        	          abortTransaction (transactionId);
        	            String abortOprsn = "ABORT(" +transactionId+ ")";
        	            transaction1.inputBuffer.insertElementAt (abortOprsn, 0);
        	            x = true;                                         // Here, the transactionId should go in wait-state
        	         }
        		 }
        		 return x;
        	 }
         }
         System.out.println("this is the locking transaction which is getting null : "+lockingTransactionId );
         if (age < tm.activeTransactions.get (lockingTransactionId).ageOfTransaction ) {         // THIS transaction is NOT younger
            tm.dispMsg ("Site" +siteID, transactionId+ "(" +age+ ") is NOT Younger than locking " +lockingTransactionId+ "(" +tm.activeTransactions.get(lockingTransactionId).ageOfTransaction+ ")");
            String repeatOprsn = "w(" +transactionId+ ",X" +variableIndex+ "," +value+ ")";
            transaction1.inputBuffer.insertElementAt (repeatOprsn, 0);
//            tm.sleep (100, "Site" +siteID+ " : Waiting for " +transactionId+ " to write X" +variableIndex,
//                           "Site" +siteID+ " : Waiting for " +transactionId+ " to write X" +variableIndex);
            return (true);                                                                      // Here, the transactionId should go in wait-state
         } else {                                                                               // THIS transaction is younger
            tm.dispMsg ("Site" +siteID, transactionId+ "(" +age+ ") is Younger than locking " +lockingTransactionId+ "(" +tm.activeTransactions.get(lockingTransactionId).ageOfTransaction+ ")");
//          abortTransaction (transactionId);
            String abortOprsn = "ABORT(" +transactionId+ ")";
            transaction1.inputBuffer.insertElementAt (abortOprsn, 0);
            return (true);                                           // Here, the transactionId should go in wait-state
         }
      }
   }

   public boolean canRead (int variableIndex, boolean readOnlyTrans, String transactionId) {
      if (state != 4 && state != 3) return false;
      if (readOnlyTrans) return true;
      if (variables [variableIndex].lockState == 2) {
         if (variables [variableIndex].isWriteLockedBy.equals (transactionId)) return true;
         else 
         {
        	 Transaction transaction1 = tm.activeTransactions.get (transactionId);
             int age = transaction1.ageOfTransaction;

        	 Variable transVar = variables [variableIndex];
             String lockingTransactionId = transVar.isWriteLockedBy;
        	 if (age < tm.activeTransactions.get (lockingTransactionId).ageOfTransaction) {         // THIS transaction is NOT younger
                 tm.dispMsg ("Site" +siteID, transactionId+ "(" +age+ ") is NOT Younger than locking " +lockingTransactionId+ "(" +tm.activeTransactions.get(lockingTransactionId).ageOfTransaction+ ")");
                 String repeatOprsn = "r(" +transactionId+ ",X" +variableIndex+ ")";
                 transaction1.inputBuffer.insertElementAt (repeatOprsn, 0);
//                 tm.sleep (100, "Site" +siteID+ " : Waiting for " +transactionId+ " to write X" +variableIndex,
//                                "Site" +siteID+ " : Waiting for " +transactionId+ " to write X" +variableIndex);
                 return (true);                                                                      // Here, the transactionId should go in wait-state
              } else {                                                                               // THIS transaction is younger
                 tm.dispMsg ("Site" +siteID, transactionId+ "(" +age+ ") is Younger than locking " +lockingTransactionId+ "(" +tm.activeTransactions.get(lockingTransactionId).ageOfTransaction+ ")");
//               abortTransaction (transactionId);
                 String abortOprsn = "ABORT(" +transactionId+ ")";
                 transaction1.inputBuffer.insertElementAt (abortOprsn, 0);
                 return (true);                                           // Here, the transactionId should go in wait-state
              }
        	 
         }
      }
      if (state == 4) return true;
      if (state == 3) {
         if (hasUniqueVar) return true;
         else {
            tm.debugMsg ("Site" +siteID, "in Recovery State - CANNOT Read X"+ variableIndex);
            return false;
         }
      }
      return false;
   }

   public void run () {
      state = 4;
      tm.dispMsg ("Site" +siteID, "Started running");
      while (true) {
         synchronized (inputBuffer) {
            while (inputBuffer.size() == 0) {
               try {
                  tm.dispMsg ("Site" +siteID, "waiting for command in inputBuffer");
                  inputBuffer.wait();
               } catch (InterruptedException ie) {
                  tm.dispMsg ("Site" +siteID, "while waiting for inputBuffer : " +ie);
               }
            }
            transOperation = inputBuffer.get (0);
            inputBuffer.remove(0);
            inputBuffer.notify();
         }
         isRunning = true;
         if (tm.DEBUG) {
         tm.debugMsg ("Site" +siteID, "received operation in the Input Buffer = " +transOperation);
         }
         performOperation (transOperation);
         isRunning = false;
      }
   }

   public void performOperation (String transOperation) {
      tm.debugMsg ("Site" +siteID, "operation = " + transOperation);
      String operation = transOperation.substring (0, transOperation.indexOf ("("));
      transOperation = transOperation.substring (transOperation.indexOf ("(") +1);
      if (state != 4 && state != 3) {
         if (operation.equals ("recover")) {
            for (int v1=1; v1<tm.NOV; v1++)
                variables [v1] = new Variable (v1);
            state = 3;
            siteAge = tm.ticker;
            for (int s1 = 1; s1 < tm.NOS; s1++) {
               Site site1 = tm.sites [s1];
               if (site1.state == 4) {
                  for (int v1 = 2; v1 < tm.NOV; v1++) {
                     if (site1.variables [v1].lockState < 2) {
                        this.variables [v1] = site1.variables [v1];
                     }
                     v1++;
                  }
                  break;
               }
            }
            System.out.println("Site" +siteID+ "Recovery Started ! \t(Site State = " +state+ ")");
            return;
         } 
      }
// * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
      if (operation.equals ("dumpAllItems")) {
         for (int v1=1; v1<tm.NOV; v1++) {
            if (tm.variableDefinedAtSite [v1] [siteID]) {
               Variable transVar = variables [v1];
               tm.dispMsg ("Site" +siteID, "Value of X" +v1+ " = "+ transVar.commitValue+ " / " +transVar.variableValue+ "\t Write Locked By (" +transVar.isWriteLockedBy+ ") \t\t\t Var State " +transVar.lockState);
            }
         }
         return;
      } else  if (operation.equals ("dumpSingleItem")) {
         int variableIndex = Integer.parseInt (transOperation.substring (0, transOperation.indexOf (")")));
         Variable transVar = variables [variableIndex];
         tm.dispMsg ("Site" +siteID, "Value of X" +variableIndex+ " = "+ transVar.commitValue+ " / " +transVar.variableValue+ "\t Write Locked By (" +transVar.isWriteLockedBy+ ") \t\t\t Var State " +transVar.lockState);
         return;
      } else if (operation.equals ("fail")) {
         variables = new Variable [tm.NOV];
         inputBuffer = new Vector <String> ();
         state = 1;
         
         tm.dispMsg ("Site" +siteID, "fails !");
         return;
      }else if (operation.equals ("recover")) {
         tm.dispMsg ("Site" +siteID, "Already Running !");
         return;
      } else if (operation.equals ("COPY")) {   // recovery by Copying Even Indexed Variables
          int fromSiteNum = Integer.parseInt (transOperation.substring (0, transOperation.indexOf (")")));
          tm.dispMsg ("Site" +siteID, "Recovery - Copying from Site" +fromSiteNum);
          Site fromSite = tm.sites [fromSiteNum];
          for (int v1=2; v1<tm.NOV; v1++) {
             variables [v1] = fromSite.variables [v1];
             v1++;                               // Even Indexed Variables
          }
          state = 4;
          return;
      } else {
         tm.dispMsg ("Site" +siteID, "No Such Operation : " +operation);
         return;
      }
   }

}