import java.util.Date;

public class test {
	
	public static void main(String[] args) {
		java.util.Date date = new java.util.Date();
		Date utilDate = new Date(System.currentTimeMillis());
		java.sql.Date sqlDate = new java.sql.Date(date.getTime());
		System.out.println(sqlDate);
	}
}
