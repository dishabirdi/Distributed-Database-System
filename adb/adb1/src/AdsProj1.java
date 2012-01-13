public class AdsProj1 {

   public static void main (String[] args) {
      String inputFile = "C:/Users/KM. DISHA/command.txt";
      boolean debug = false;
      int NOS = 10+1;
      try {
         inputFile = args [0];
      } catch (ArrayIndexOutOfBoundsException aioobe) {
         inputFile = "C:/Users/KM. DISHA/command.txt";
      }
      try {
         if (args [1].equalsIgnoreCase ("Y")) {
            debug = true;
         } else {
            debug = false;
         }
      } catch (ArrayIndexOutOfBoundsException aioobe) {
         debug = false;
      }
      try {
         NOS = Integer.parseInt (args [2]);
         NOS++;
      } catch (ArrayIndexOutOfBoundsException aioobe) {
         NOS = 10+1;
      }
      if (debug) System.out.println ("DEBUG = True");
      else       System.out.println ("DEBUG = False");
      System.out.println ("No. of SItes = " +(NOS-1));
      TransactionManager tm = new TransactionManager (inputFile, debug, NOS);
      tm.startParser();
   }
}