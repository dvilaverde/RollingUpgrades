package org.jgroups.common.demo;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;

public class NumberPrinter implements Serializable {

  public static final long serialVersionUID = 21L;

  public static int print(int number) throws Exception {
    System.out.println("Received: " + String.valueOf(number));
    return number * 2;
  }


  public static void main(String[] args) {
    try (FileInputStream fis = new FileInputStream("./TEST0.bin");) {
      ObjectInputStream o = new ObjectInputStream(fis);
      Object ob = o.readObject();
      System.out.println(ob.getClass());
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }
}
