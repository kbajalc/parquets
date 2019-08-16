import java.math.BigDecimal;

class Test {
  public static void main(String[] args) {
    var big = new BigDecimal(-254);

    byte[] raw = big.unscaledValue().toByteArray();
    for(int i=0;i<raw.length;i++)
    System.out.println(raw[i] & 0xff);
  }
}