package edu.cooper.ece460.http;

import java.io.FileNotFoundException;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;


// Should probably be in a jsp
public class MailSorterServlet extends HttpServlet {
    PrintWriter out;

    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException
    {
        resp.setContentType("text/html");
        resp.setStatus(HttpServletResponse.SC_OK);
        String user = req.getParameter("user");
        String host = req.getParameter("host");
        String dataTableStr = req.getParameter("datatable");

        out = resp.getWriter();
        out.println("<link rel='stylesheet' href='style.css' type='text/css'>");
        out.println("<h1>Sort Mail</h1>");

        req.getRequestDispatcher("/mail_sort.html").include(req, resp); 

        if ((user == null && host == null) || dataTableStr == null)
            return;

        HTablePool pool = new HTablePool();
        HTableInterface dataTable = null;
        try{
            dataTable = pool.getTable(dataTableStr);
        }
        catch(RuntimeException e){
            out.println("Table <tt>" + dataTableStr + "</tt> not found");
            return;
        }

        out.println("<div style='float:left; padding-left:10px;'>");

        Scan s = new Scan();
        FilterList flist = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter filter1 =
            new SingleColumnValueFilter("userhost".getBytes(),
                                        "user".getBytes(),
                                        CompareFilter.CompareOp.EQUAL,
                                        new RegexStringComparator(user));
        flist.addFilter(filter1);
        SingleColumnValueFilter filter2 =
            new SingleColumnValueFilter("userhost".getBytes(),
                                        "host".getBytes(),
                                        CompareFilter.CompareOp.EQUAL,
                                        new RegexStringComparator(host));
        flist.addFilter(filter2);
        s.setFilter(flist);

        ResultScanner rs = dataTable.getScanner(s);
        out.println("User:" + user);
        out.println("Host:" + host);

        // if (outLines.size() == 0) {
        //     out.println("no results found <tt>" + dataTable + "</tt>");
        //     return;
        // }

        out.println("<table><tr><th>User</th><th>Host</th><th>Local logins</th><th>SSH logins</th><th>Failed logins</th></tr>");
        for(Result rr : rs){
            String userString = Bytes.toString(rr.getValue("userhost".getBytes(), "user".getBytes()));
            String hostString = Bytes.toString(rr.getValue("userhost".getBytes(), "host".getBytes()));
            int local = Bytes.toInt(rr.getValue("userhost".getBytes(), "local".getBytes()));
            int ssh = Bytes.toInt(rr.getValue("userhost".getBytes(), "ssh".getBytes()));
            int failed = Bytes.toInt(rr.getValue("userhost".getBytes(), "failed".getBytes()));

            out.println("<tr>");
            out.print("<td><a href='?datatable=" + dataTableStr + "&user=" + userString + "&host=(.*)" + "'>" + userString + "</a></td>");
            out.print("<td><a href='?datatable=" + dataTableStr + "&user=(.*)" + "&host=" + hostString + "'>" + hostString + "</a></td>");
            out.print("<td class='center'>" + local + "</td>");
            out.print("<td class='center'>" + ssh + "</td>");
            out.print("<td class='center'>" + failed + "</td>");
            out.println("</tr>");
        }
        out.println("</div>");

        pool.close();
    }
}

