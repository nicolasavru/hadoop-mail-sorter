package edu.cooper.ece460.http;

import java.util.Scanner;
import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.lang.Process;
import java.lang.ProcessBuilder;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class ReadResultsServlet extends HttpServlet {
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
    {
        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("<h1>Hello Servlet</h1>");
        response.getWriter().println("session=" + request.getSession(true).getId());
    }

    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException
    {
        resp.setContentType("text/html");
        resp.setStatus(HttpServletResponse.SC_OK);
        final String mainClass = "edu.cooper.ece460.mailsorter.HadoopMailSorter";

        String inputDir = req.getParameter("input");
        String outputDir = req.getParameter("output");
        String labelindex = req.getParameter("labelindex");
        String jar = "target/HadoopMailSorter-1.0.jar"; // Should probably not be hardcoded

        PrintWriter out = resp.getWriter();
        if (inputDir == null || outputDir == null || labelindex == null) {
            out.println("Invalid parameters");
            System.exit(-1);
        }

        // Remove existing output directories
        new ProcessBuilder(
            "hadoop", "fs", "-rmr",
            outputDir
        ).start();

        Process pb = new ProcessBuilder(
            "hadoop", "jar",
            jar, mainClass, "readResults", inputDir, labelindex, outputDir
        ).start();

        // Display output and error messages
        out.println("<div>");
        out.println("<tt>");
        BufferedReader hadoopOut = new BufferedReader(new InputStreamReader(pb.getInputStream()));
        BufferedReader hadoopError = new BufferedReader(new InputStreamReader(pb.getErrorStream()));

        String line = null;
        while((line = hadoopError.readLine()) != null) {
            out.println(line + "<br />");
            out.flush();
        }

        out.println("</tt></div>");

        // Merge output directory files
        new ProcessBuilder(
            "hadoop", "fs", "-getmerge",
            outputDir, outputDir).start();
    }

    public String nl2br(String s) {
        return s.replaceAll("(\r\n|\n)", "<br />");
    }

    public String convertStreamToString(java.io.InputStream is) {
        java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }
}
