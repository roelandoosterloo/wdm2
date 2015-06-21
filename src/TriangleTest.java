
public class TriangleTest {
	
	public static void main(String[] args) {
		Edge a = new Edge(1,2);
		Edge b = new Edge(3,2);
		Edge c = new Edge(1,3);
		Edge d = new Edge(1,4);
		
		Triangle t = new Triangle();

		t.trySetB(b);
		System.out.println(t.isTriangle());
		t.trySetA(a);
		System.out.println(t.isTriangle());
		t.trySetC(c);
		System.out.println(t.isTriangle());
		System.out.println(t.trySetC(d));
		System.out.println(t.isTriangle());
	}

}
