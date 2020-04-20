package dbf0.disk_key_value.readwrite.btree;

import java.util.Random;
import java.util.stream.IntStream;

/**
 * Experimentation with B+Trees
 */
public class BPTreeExp {

  public static void main(String[] args) throws Exception {
    var tree = new BTree<Integer, Integer>(16);
    var random = new Random(0xCAFE);
    IntStream.range(0, 1000).forEach(i -> tree.put(random.nextInt(100000), i));

    System.out.println();
    tree.getRoot().recursivelyPrint(0);
  }
}
