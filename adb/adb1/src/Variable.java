import java.util.ArrayList;
import java.util.Hashtable;

public class Variable {
   public int variableIndex;
   public String variableName;
   public int variableAge;

   public String isWriteLockedBy;

   public int variableValue;
   public int commitValue;
   public int lockState;
   public ArrayList<String> isReadLockedBy = new ArrayList<String>();
   public Hashtable <String, Integer> variableCopies;

   public Variable (int variableIndex) {
      this.variableIndex = variableIndex;
      variableName = "x" + variableIndex;
      variableAge = 0;
      commitValue = 10 * variableIndex;
      variableValue = commitValue;
      lockState = 0;
      isWriteLockedBy = new String ("");
      variableCopies = new Hashtable <String, Integer> ();
   }
}