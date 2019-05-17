public class Starter {

    public static void main (String[] args){
        try {
            if (args.length != 3) {
                System.out.println("\nInsert arguments in this order: " +
                        "1. Query name ', 2. 'file city-attributes, 3. specific query file'");
            }
            if (args[0].equalsIgnoreCase("Query 1"))
                Query1.main(args);
            if (args[0].equalsIgnoreCase("Query 3"))
                Query3.main(args);
            if(!args[0].equalsIgnoreCase("Query 1") && !args[0].equalsIgnoreCase("Query 3") )
                System.out.println("\nInsert correct Query name");
        }catch (Exception e){
            System.out.println("\nInsert correct arguments");
        }
    }
}
