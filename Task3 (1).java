
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

public class Task3 {

	public static void main(String[] args) throws IOException{

		
		
		
		Scanner in = new Scanner(new FileReader("mapresults/tdp1/part-r-00000"));

		String m1= new String();
		String m2= new String();
		String m3= new String();
		String m4= new String();
		String m5= new String();
		String m6= new String();
		String m7= new String();
		String m8 = new String();
		
		int v1 = 0, v2=0, v3=0, v4=0, v5=0, v6=0, v7=0, v8 = 0;
		
		
		while(in.hasNext()) {

			String str = in.nextLine();
			if(str.equals("")) continue;
			
			
			if(str.startsWith("m1_")) {
				int i = Math.max(str.lastIndexOf((char)9), str.lastIndexOf(" "));
				String s = str.substring(3, i);
				int v = Integer.parseInt(str.substring(i+1, str.length()));
				
				if(v>v1) {
					v1 = v;
					m1 = s;
				}
			}
			else if(str.startsWith("m2_")) {
				int i = Math.max(str.lastIndexOf((char)9), str.lastIndexOf(" "));
				String s = str.substring(3, i);
				int v = Integer.parseInt(str.substring(i+1, str.length()));
				if(v>v2) {
					v2 = v;
					m2 = s;
				}
			}
			else if(str.startsWith("m3_")) {
				int i = Math.max(str.lastIndexOf((char)9), str.lastIndexOf(" "));
				String s = str.substring(3, i);
				int v = Integer.parseInt(str.substring(i+1, str.length()));
				
				if(v>v3) {
					v3 = v;
					m3 = s;
				}
				
			}
			else if(str.startsWith("m4_")) {
				int i = Math.max(str.lastIndexOf((char)9), str.lastIndexOf(" "));
				String s = str.substring(3, i);
				int v = Integer.parseInt(str.substring(i+1, str.length()));
				
				if(v>v4) {
					v4 = v;
					m4 = s;
				}				
			}
			else if(str.startsWith("m5_")) {
				int i = Math.max(str.lastIndexOf((char)9), str.lastIndexOf(" "));
				String s = str.substring(3, i);
				int v = Integer.parseInt(str.substring(i+1, str.length()));
				
				if(v>v5) {
					v5 = v;
					m5 = s;
				}
				
			}
			else if(str.startsWith("m6_")) {
				int i = Math.max(str.lastIndexOf((char)9), str.lastIndexOf(" "));
				String s = str.substring(3, i);
				int v = Integer.parseInt(str.substring(i+1, str.length()));
				
				if(v>v6) {
					v6 = v;
					m6 = s;
				}
			}
			else if(str.startsWith("m7_")) {
				int i = Math.max(str.lastIndexOf((char)9), str.lastIndexOf(" "));
				String s = str.substring(3, i);
				int v = Integer.parseInt(str.substring(i+1, str.length()));
				
				if(v>v7) {
					v7 = v;
					m7 = s;
				}
				
			}
			else if(str.startsWith("m8_")) {
				int i = Math.max(str.lastIndexOf((char)9), str.lastIndexOf(" "));
				String s = str.substring(3, i);
				int v = Integer.parseInt(str.substring(i+1, str.length()));
				
				if(v>v8) {
					v8 = v;
					m8 = s;
				}
			}
		}
		
		System.out.println(m1 + "  " + v1);
		
		System.out.println(m2 + " " + v2);
		
		System.out.println(m3 + " " + v3);
		
		System.out.println(m4 + " " + v4);
		
		System.out.println(m5 + " " + v5);
		
		System.out.println(m6 + " " + v6);
		
		System.out.println(m7 + " " + v7);
		
		System.out.println(m8 + " " + v8);
		
		
	}

}
