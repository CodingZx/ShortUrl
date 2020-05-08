package lol.cicco.url.util;

public class ConversionUtils {
    private static final String CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private static final int SCALE = 62;

    public static String encode(long num) {
        StringBuilder sb = new StringBuilder();
        int remainder = 0;

        while (num > SCALE - 1) {
            remainder = Long.valueOf(num % SCALE).intValue();
            sb.append(CHARS.charAt(remainder));

            num = num / SCALE;
        }

        sb.append(CHARS.charAt(Long.valueOf(num).intValue()));
        return sb.reverse().toString();
    }

    public static long decode(String str) {
        try {
            long num = 0;
            for (int i = 0; i < str.length(); i++) {
                int index = CHARS.indexOf(str.charAt(i));
                num += (long) (index * (Math.pow(SCALE, str.length() - i - 1)));
            }
            return num;
        }catch (Exception e) {
            return 0;
        }
    }
}
