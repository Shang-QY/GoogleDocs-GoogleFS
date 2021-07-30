package gdoc.entity;

public class Sheet {
    public class Cell{
        public class V{
            class Ct{
                public String fa;
                public String t;
            }
            public Ct ct = new Ct();
            public String m;
            public String v;
        }
        public int r;
        public int c;
        public V v = new V();
    }
    public String name;
    public Cell[] celldata;

    public void initcelldata(String[] body)
    {
        celldata = new Cell[body.length];
        for(int i=0;i<body.length;i++){
            celldata[i] = new Cell();
            String s = body[i];
            String[] tmp = s.split(":");
            String[] index = tmp[0].split(",");
            celldata[i].r = Integer.parseInt(index[0]);
            celldata[i].c = Integer.parseInt(index[1]);
            celldata[i].v.ct.fa = "General";
            celldata[i].v.ct.t = "g";
            celldata[i].v.m = tmp[1];
            celldata[i].v.v = tmp[1];
        }
    }
}
