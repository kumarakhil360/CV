package com.uhc.optum.icm.webservice;

import com.uhc.optum.dao.MainDao;
import com.uhc.optum.icm.sync.IcmSync;
import com.uhc.optum.icm.sync.IcmSyncDao;
import com.uhc.optum.icm.sync.IcmSyncRun;
import com.uhc.optum.util.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.*;

// Suppress "String literals should not be duplicated" warning for Sonar
@java.lang.SuppressWarnings("java:S1192")
public class IcmDailyJobStatus {

    private static final Logger LOGGER = LogManager.getLogger( IcmDailyJobStatus.class );

    private  boolean isBatchCompleted = false;
    private  LocalDate mondayAfterThirdSunday ;
    private  LocalDate thursdayBeforeThirdSaturday ;


    IcmDao dao = new IcmDao( MainDao.DATABASE );
    IcmSyncDao sdao = new IcmSyncDao( MainDao.DATABASE );
    List<Date>payAllFDates ;

    private final int hours = dao.getConfigNumeric( Config.ICM_DAILY_JOB_STATUS_HOURS );
    private final String time = dao.getConfig( Config.ICM_DAILY_JOB_STATUS_TIME );
    private final String from = dao.getConfig( Config.ICM_DAILY_JOB_STATUS_FROM );
    private final String to = dao.getConfig( Config.ICM_DAILY_JOB_STATUS_TO );
    private final String cc = dao.getConfig( Config.ICM_DAILY_JOB_STATUS_CC );
    private final String cspholidayList = dao.getConfig(Config.ICM_DAILY_CSP_HOLIDAYLIST);
    private final Map<Integer, IcmJob> jobMap = new HashMap<>();
    private LocalDateTime startDateTime = null;
    private List<String> excludedItems;
    private static final DateTimeFormatter mmddyyyyhhmmssa = DateTimeFormatter.ofPattern( DateUtil.FMT_MMDDYYYY_HHMMSSA, Locale.US );

    /**
     * Main method.
     * @param args String[]
     */
    public static void main( String[] args ) {
        LOGGER.info( "IcmDailyJobStatus.main - start" );
        try {
            IcmDailyJobStatus js = new IcmDailyJobStatus();
            js.processArgs(args);
            js.run( args );
        }
        catch( Exception e ) {
            LOGGER.error( "Exception: " , e );
            LogUtil.printStackTrace( e );
            System.exit( -1 );
        }
        LOGGER.info( "IcmDailyJobStatus.main - stop" );
    }

    private void processArgs(String[] args) {
        try {
            for (String arg : args) {
                LOGGER.info("arg: {}", arg);
                if (arg.contains("=")) {
                    String name = arg.substring(0, arg.indexOf("="));
                    String value = arg.substring(arg.indexOf("=") + 1);
                    if ("payallfdates".equalsIgnoreCase(name)) {
                        LOGGER.info("payallfdates: {}", value);
                        this.payAllFDates = getPayAllFDates(value);
                        LOGGER.info(" Pay out All F Dates : {}" ,payAllFDates );
                    }
                }
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

        private List<Date> getPayAllFDates(String value) throws ParseException {
            List<String> allFDates = Arrays.asList(value.split("#"));
            List<Date> payAllFDate  = new ArrayList<>();
            SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");

            for(String allFDateString : allFDates){
                Date allFDate = sdformat.parse(allFDateString);

                payAllFDate.add(allFDate);

            }

            return payAllFDate;

        }
    /**
     * Run process.
     * @param args String[]
     */
    private void run( String[] args ) throws IOException, ParseException {

        LOGGER.info( "args: {}" , ( args != null ? args.length : null ) );
        List<String> holidayList = Arrays.asList(cspholidayList.split("#"));


        // Calculate the start time for the email status.
      startDateTime = calculateStartTime( hours, time );
       LocalDateTime endDateTime = LocalDateTime.now();
//        startDateTime = LocalDateTime.of(2023, 12, 13, 14, 33, 48, 640000);
//        LocalDateTime endDateTime = LocalDateTime.of(2023, 12, 14, 9, 33, 48, 640000);





        MailUtil mail = new MailUtil();
        String subject = "ICM Daily Jobs Status Report - " + endDateTime.toLocalDate().format( DateTimeFormatter.ofPattern( DateUtil.FMT_MDYYYY ) )+" - "+isBatchjobsCompleted(startDateTime,endDateTime);
        mail.init( from, to, subject );

        StringBuilder msg  = new StringBuilder();

        msg.append("<table style=\"width:100%;\">")
        .append("<tr style=\"height:100px\" > <td style=\"align:left;width:20%; \">")
        .append(getHtmlImageTag(encodeImage()))
        .append("</td><td style=\"align:left\">")
        .append("<h2>ICM Daily Jobs Status Report </h2></td></tr>")
        .append("<tr><td style=\"text-align:left\" colspan =\"2\">")
        .append("<h5 style=\"color:blue\">Job Status <h5>")
        .append("</td></tr><tr  ><td colspan =\"2\" style=\"text-align:left\">")
        .append(MessageFormat.format("<b>Since:</b> {0}",startDateTime.format( mmddyyyyhhmmssa )))
        .append("</td></tr>")
        .append("</table>");

        msg.append( "<table cellspacing=\"4\" cellpadding=\"6\" style=\"font-size:12px\">" )
                .append( "<tr>" )
                .append( "<th style=\"border-bottom:2px solid black;\">Job Description</th>" )
                .append( "<th style=\"border-bottom:2px solid black;text-align:center;\">Start Time</th>" )
                .append( "<th style=\"border-bottom:2px solid black;text-align:center;\">End Time</th>" )
                .append( "<th style=\"border-bottom:2px solid black;text-align:right;\">Run Time</th>" )
                .append( "<th style=\"border-bottom:2px solid black;text-align:center;\">Status</th>" )
                .append( "</tr>" );


        LinkedHashMap<String,Integer> scheduleBatchJobs = dao.getIcmBatchJobsSchedule(startDateTime,endDateTime);

        for(Map.Entry<String,Integer> entry : scheduleBatchJobs.entrySet()) {



            int icmJobId = entry.getValue();
            IcmJob job = dao.getIcmJob(icmJobId);

//            Taking decision for some complex job Schedule


            if(job.getName().equalsIgnoreCase("enrollment_csp") && isUSHoliday(holidayList)){
                continue ;
            }

            if(job.getName().equalsIgnoreCase("ips_transactions") && !isMondayAfterThirdSunday() ){
                continue;
            }
            if(job.getName().equalsIgnoreCase("cms_comprpt_processing") && !isWednesdayAfterFirstSaturday()){
                continue;
            }
            if(job.getName().equalsIgnoreCase("acra_debtloader") && !isThursdayBeforeThirdSaturday()){
                continue;
            }

            if(job.getName().equalsIgnoreCase("united_payout_all_f")  && !payAllFDates.contains(currentDateofMonth()) ) {
                continue;
            }

            if(job.getName().equalsIgnoreCase("united_payout_all_r")  && payAllFDates.contains(currentDateofMonth())){
                continue;
            }

            List<IcmJobHistory> jobCompleted = dao.getIcmCompletedJob(startDateTime, endDateTime, icmJobId);
            if (jobCompleted != null && !(jobCompleted.isEmpty())) {
                LOGGER.info("ICM job Completed with ICM job id : {}" , icmJobId);
                msg.append(formatRecords(jobCompleted, true));
                continue;

            }
            IcmJobHistory jobInProgress = dao.getIcmInprogressJob(icmJobId);
            if (jobInProgress != null) {
                LOGGER.info("InProgress job in ICM with ICM job id : {}" , icmJobId);
                msg.append(formatRecords(jobInProgress, false));
                isBatchCompleted  = false;
                continue;

            }
            if(job != null){
                LOGGER.info("Jobs that are not yet executed  : {}" ,job.getName());
                msg.append(diaplayNotRunJob(job));
            }

        }


        // Append FDS details.
//        msg.append( getFdsDetails() );
        // Append sync details.
//        msg.append( getSyncDetails() );

        msg.append( "</table>" );

        LOGGER.info( "Sending email ..." );
        mail.sendMail( to, cc, msg.toString() );
    }



    public String isBatchjobsCompleted(LocalDateTime start,LocalDateTime end) throws ParseException {
        if(LocalDate.now().equals(mondayAfterThirdSunday) || LocalDate.now().equals(thursdayBeforeThirdSaturday)){
            isBatchCompleted = dao.getLastScheduledJobStatusIpsAndAcra(start,end);
        }else{
            isBatchCompleted = dao.getLastScheduledJobStatus(start,end);
        }

        if(!isBatchCompleted){
            return " InProgress ";
        }
        return " Success ";
    }

    public  String encodeImage() throws IOException{

        InputStream imageStream = getClass().getResourceAsStream("/images/optumlogo.jpeg");

        BufferedImage image = ImageIO.read(imageStream);
        imageStream.close();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ImageIO.write(image,"jpeg",outputStream);
        byte[] imageBytes = outputStream.toByteArray();
        String encodedImage  = Base64.getEncoder().encodeToString(imageBytes);

        outputStream.close();

        return encodedImage;
    }

    public String getHtmlImageTag(String encodedImage){

        return "<img src=\"data:image/jpeg;base64,"+encodedImage+ " \" />";

    }

    public boolean isUSHoliday(List<String>holidayList) throws ParseException {
        Date currentDate = currentDateofMonth();
        SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");

        for(String holidayDateString : holidayList){
            Date holidayDate = sdformat.parse(holidayDateString);

            if(currentDate.compareTo(holidayDate) == 0)
                return true;
        }

        return false;
    }

    public boolean isMondayAfterThirdSunday() throws ParseException {

        LocalDate currentDate = LocalDate.now();
        LocalDate thirdSunday = currentDate.with(TemporalAdjusters.nextOrSame(DayOfWeek.SUNDAY)).with(TemporalAdjusters.dayOfWeekInMonth(3,DayOfWeek.SUNDAY));
        mondayAfterThirdSunday = thirdSunday.plusDays(1);
        LOGGER.info("Monday After Third Sunday : {}",mondayAfterThirdSunday);

        return currentDate.equals(mondayAfterThirdSunday);

    }
    public boolean isThursdayBeforeThirdSaturday() throws ParseException {

        LocalDate currentDate = LocalDate.now();
        LocalDate thirdSaturday = currentDate.with(TemporalAdjusters.nextOrSame(DayOfWeek.SATURDAY)).with(TemporalAdjusters.dayOfWeekInMonth(3,DayOfWeek.SATURDAY));
        thursdayBeforeThirdSaturday = thirdSaturday.minusDays(2);
        LOGGER.info("Thursday Before Third Saturday : {} ",thursdayBeforeThirdSaturday);

        return currentDate.equals(thursdayBeforeThirdSaturday);

    }
    public boolean isWednesdayAfterFirstSaturday()  {

        LocalDate currentDate = LocalDate.now();
        LocalDate firstSaturday = currentDate.with(TemporalAdjusters.nextOrSame(DayOfWeek.SATURDAY)).with(TemporalAdjusters.dayOfWeekInMonth(1,DayOfWeek.SATURDAY));
        LocalDate wednesdayAfterFirstSaturday = firstSaturday.plusDays(4);
        LOGGER.info("Wednesday After First Saturday : {}",wednesdayAfterFirstSaturday);

        return currentDate.equals(wednesdayAfterFirstSaturday);

    }
    public Date currentDateofMonth() throws ParseException {
        String currentDateString= new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
        Date currentDate = sdformat.parse(currentDateString);

        System.out.println("current Date of month : >>>>>>>>>>> "+currentDate );

        return currentDate;
    }


    /**
     * Calculate start time.
     * @param hours int
     * @param time  String
     * @return LocalDateTime
     */
    private LocalDateTime calculateStartTime( int hours, String time ) {
        LOGGER.info( "Calculating start date/time: [hours:" + hours + "][time:" + time + "]" );
        // If hours is set, minus hours from current time.
        LocalDateTime startDateTime;
        if( hours > 0 ) {
            startDateTime = LocalDateTime.now().minusHours( hours );
        }
        // Else, use time.
        else {
            // If time is not set, default to 5pm.
            if( !StringUtil.isValid( time ) ) {
                time = "17:00";
            }
            // Default to 5:00pm.
            int hour = 17;
            int min = 0;
            // Parse out hour and minute.
            if( time.contains( ":" ) ) {
                String h = time.substring( 0, time.indexOf( ":" ) );
                if( StringUtil.isNumeric( h ) ) {
                    hour = Integer.parseInt( h );
                }
                String m = time.substring( time.indexOf( ":" ) + 1 );
                if( StringUtil.isNumeric( m ) ) {
                    min = Integer.parseInt( m );
                }
            }
            else {
                hour = Integer.parseInt( time );
            }
            // Start with current date/time.
            startDateTime = LocalDateTime.now();
            // Minus 1 day.
            startDateTime = startDateTime.minusDays( 1 );

            // Set hours, minutes and seconds.
            startDateTime = startDateTime.withHour( hour ).withMinute( min ).withSecond( 0 ).withNano( 0 );
        }
        LOGGER.info( "Returning start date/time: " + startDateTime );
        return startDateTime;
    }

    /**
     * Format records.
     * @param history  List
     * @param complete boolean
     * @return String
     */

//    Modified Method
    private String formatRecords( IcmJobHistory history, boolean complete ) {

        if( excludedItems == null ) {
            excludedItems = new ArrayList<>();
            excludedItems.add( "commissionstatement_csv" );
            excludedItems.add( "downlinehierarchycsv" );
            excludedItems.add( "downlinehierarchypdf" );
            excludedItems.add( "downlinehierarchyxls" );
            excludedItems.add( "productionsummarycsv" );
            excludedItems.add( "productionsummarypdf" );
            excludedItems.add( "productionsummaryxls" );
            excludedItems.add( "statementscsv" );
            excludedItems.add( "statementspdf" );
            excludedItems.add( "statementsxls" );
            excludedItems.add( "GetUpdatesForSync" );
        }


        // Iterate over the history and filter out excluded items.

            IcmJob job = jobMap.computeIfAbsent( history.getJobId(), i -> dao.getIcmJob( i ) );

            if( history.getEnd() == null && history.getStart().isBefore( startDateTime ) ) {
                LOGGER.warn( "Found in process job that started before start of window: {}" , history.getJobId() );
            }


        if( history==null && complete ) {
            // Add empty record.
            return "<tr valign=\"top\">" +
                    "<td>ICM Jobs</td>" +
                    "<td colspan=\"4\" align=\"center\">No jobs run during report window</td>" +
                    "</tr>";
        }




        StringBuilder msg = new StringBuilder();




                // Get data.
                String desc = job.getDesc();
                LocalDateTime start = history.getStart();
                LocalDateTime end = history.getEnd();

                // Returns a string and a decoration for the string.
                String[] sts = getPayoutStatus( history ,desc);
                String status = sts[0];
                String decor = null;
                if( sts.length == 2 ) {
                    decor = sts[1];
                }

                // Add row to report.
                msg.append( formatRow( desc, start, end, status, decor ) );



        return msg.toString();
    }

    private String diaplayNotRunJob( IcmJob job) {

        StringBuilder msg = new StringBuilder();


        // Get data.
        String desc = job.getDesc();


        // Add row to report.
        msg.append( formatRowNotRunJob( desc ) );



        return msg.toString();
    }

    private String formatRowNotRunJob(String desc){
        String row = "<tr valign=\"top\">"
                +"<td>{0}</td>";

        return MessageFormat.format(row,desc);

    }

    /*
    private String formatBackGroundColor(String status){
        if(status.equalsIgnoreCase("inprogress"))
            return

    }

     */


    private String formatRecords( List<IcmJobHistory> history, boolean complete ) {

        if( excludedItems == null ) {
            excludedItems = new ArrayList<>();
            excludedItems.add( "commissionstatement_csv" );
            excludedItems.add( "downlinehierarchycsv" );
            excludedItems.add( "downlinehierarchypdf" );
            excludedItems.add( "downlinehierarchyxls" );
            excludedItems.add( "productionsummarycsv" );
            excludedItems.add( "productionsummarypdf" );
            excludedItems.add( "productionsummaryxls" );
            excludedItems.add( "statementscsv" );
            excludedItems.add( "statementspdf" );
            excludedItems.add( "statementsxls" );
            excludedItems.add( "GetUpdatesForSync" );
        }

        List<IcmJobHistory> keepers = new ArrayList<>();

        // Iterate over the history and filter out excluded items.
        for( IcmJobHistory h : history ) {
            IcmJob job = jobMap.computeIfAbsent( h.getJobId(), i -> dao.getIcmJob( i ) );
            if( excludedItems.contains( job.getName() ) ) {
                continue;
            }

            if( h.getEnd() == null && h.getStart().isBefore( startDateTime ) ) {
                LOGGER.warn( "Found in process job that started before start of window: " + h.getJobId() );
            }

            keepers.add( h );
        }

        if( keepers.isEmpty() && complete ) {
            // Add empty record.
            return "<tr valign=\"top\">" +
                    "<td>ICM Jobs</td>" +
                    "<td colspan=\"4\" align=\"center\">No jobs run during report window</td>" +
                    "</tr>";
        }


        StringBuilder msg = new StringBuilder();

        // Iterate over the keepers.
        for( IcmJobHistory h : keepers ) {

            try {
                IcmJob job = jobMap.computeIfAbsent( h.getJobId(), i -> dao.getIcmJob( i ) );

                // Get data.
                String desc = job.getDesc();
                LocalDateTime start = h.getStart();
                LocalDateTime end = h.getEnd();

                // Returns a string and a decoration for the string.
                String[] sts = getPayoutStatus( h,desc );
                String status = sts[0];
                String decor = null;
                if( sts.length == 2 ) {
                    decor = sts[1];
                }

                // Add row to report.
                msg.append( formatRow( desc, start, end, status, decor ) );
            }
            catch( Exception e ) {
                LOGGER.error( "Could not format job history: " , e );
                LogUtil.printStackTrace( e );
            }
        }

        return msg.toString();
    }

    /**
     * Get the history status.
     * @param h IcmJobHistory
     * @return String[]
     */
    private String[] getStatus( IcmJobHistory h ) {
        String status = "...";
        String decor = "<span style=\"color:red;\">{0}</span>";
        if( h.getEnd() != null ) {
            // Complete
            status = ( h.isSuccess() ? "Complete" : "Error" );
            decor = ( h.isSuccess() ? "<span style=\"color:green;\">{0}</span>" : "<span style=\"color:red;\">{0}</span>" );
        }
        else {
            // In Progress
            // Get the current task.
            String task = getTaskName( h );
            if( StringUtil.isValid( task ) ) {
                // Add task to report.
                status = "Task[" + task + "]";
                decor = "<span style=\"color:red;\">{0}</span>";
            }
        }
        return new String[]{ status, decor };
    }
// Created By Akhil
    private String[] getPayoutStatus( IcmJobHistory h ,String desc) {
        String status = "...";
        String decor = "<span style=\"color:red;\">{0}</span>";
        if( h.getEnd() != null && desc.toLowerCase().contains("payout") ) {
            // Complete
            status = ( h.isSuccess() ? "Complete" : "Error" );
            decor = ( h.isSuccess() ? "<span style=\"color:white;background:green\">{0}</span>" : "<span style=\"background:red;color:white;\">{0}</span>" );
        }else if(h.getEnd() != null){
            status = ( h.isSuccess() ? "Complete" : "Error" );
            decor = ( h.isSuccess() ? "<span style=\"color:green;\">{0}</span>" : "<span style=\"color:red;\">{0}</span>" );
        }
        else {

            // In Progress
            // Get the current task.
            String task = getTaskName( h );
            if( StringUtil.isValid( task ) ) {
                // Add task to report.
                status = "Task[" + task + "]";
                if(desc.toLowerCase().contains("payout")){
                    decor = "<span style=\"background:yellow;color:red;\">{0}</span>";
                }
                else decor = "<span style=\"color:red;\">{0}</span>";
            }
        }
        return new String[]{ status, decor };
    }

    // Ends here




    /**
     * Get active task name for job history.
     * @param h IcmJobHistory
     * @return String
     */
    private String getTaskName( IcmJobHistory h ) {

        String task = "In Progress";

        if( h == null ) {
            LOGGER.warn( "getTaskName: history is null!" );
            return task;
        }
        List<IcmJobTaskHistory> jobTaskHistory = dao.getIcmJobTaskHistory( h.getId() );
        if( jobTaskHistory == null ) {
            LOGGER.warn( "getTaskName: history list is null for: " + h.getId() );
            return task;
        }

        LOGGER.info( "jobTaskHistory: " + jobTaskHistory.size() );

        int taskId = 0;
        int lastTaskId = 0;
        // Iterate over the history tasks.
        for( IcmJobTaskHistory jth : jobTaskHistory ) {
            LOGGER.info( "jobTaskHistory: " + jth );
            // Find task with no end date.
            if( jth.getEnd() == null ) {
                taskId = jth.getIcmJobTaskId();
                LOGGER.info( "task has no end date: " + taskId );
                break;
            }
            if( lastTaskId == 0 || jth.getIcmJobTaskId() > lastTaskId ) {
                taskId = jth.getIcmJobTaskId();
                LOGGER.info( "task has end date: " + taskId );
            }
            lastTaskId = jth.getIcmJobTaskId();
        }
        LOGGER.info( "found task id: {}" ,taskId );

        if( taskId == 0 ) {
            LOGGER.warn( "getTaskName: no active tasks found for: " + h.getId() );
            return task;
        }

        LOGGER.info( "Getting job/task: " + h.getJobId() + "/" + taskId );
        IcmJobTask jobTask = dao.getIcmJobTask( h.getJobId(), taskId );
        if( jobTask == null ) {
            LOGGER.warn( "getTaskName: task not found for: " + h.getJobId() + "/" + taskId );
            return "Not Found";
        }

        task = jobTask.getTaskName() + "/" + taskId;
        LOGGER.info( "getTaskName: {}" , task );
        return task;
    }

    /**
     * Format row.
     * @param name   String
     * @param start  LocalDateTime
     * @param end    LocalDateTime
     * @param status String
     * @return String
     */
    private String formatRow( String name, LocalDateTime start, LocalDateTime end, String status, String statusDecor ) {
        // Calculate total time.
        long milliseconds = Duration.between( start, ( end != null ? end : LocalDateTime.now() ) ).toMillis();
        String duration = DurationFormatUtils.formatDuration( Math.abs( milliseconds ), "HH:mm:ss", true );
        // Name | StartDate | EndDate/In Progress | Time | Complete/Error/TaskName
        String row = "<tr valign=\"top\">" +
                "<td>{0}</td>" +
                "<td align=\"center\">{1}</td>" +
                "<td align=\"center\" " + ( end == null ? " style=\"color:red\"" : "" ) + ">{2}</td>" +
                "<td align=\"right\"  " + ( end == null ? " style=\"color:red\"" : "" ) + ">{3}</td>" +
                "<td align=\"center\">{4}</td>" +
                "</tr>";
        String sd = start.format( mmddyyyyhhmmssa );
        String ed = ( end != null ? end.format( mmddyyyyhhmmssa ) : "In Progress" );
        // Output message.                                                 0     1   2   3     4
        LOGGER.info(MessageFormat.format( "[{0}][{1}][{2}][{3}][{4}]", name, sd, ed, duration, status ) );
        if( StringUtil.isValid( statusDecor ) ) {
            status = MessageFormat.format( statusDecor, status );
        }
        // Add row to report.             0     1   2   3     4
        return MessageFormat.format( row, name, sd, ed, duration, status );
    }

    /**
     * Get FDS details for current month.
     * @return String
     */
    private String getFdsDetails() {

        StringBuilder s = new StringBuilder();

        List<IcmStatementRun> runs = dao.getStatementRun( startDateTime );
        runs = ( runs != null ? runs : new ArrayList<>() );
        LOGGER.info( "Found runs: {}" , runs.size() );

        Map<String, Integer> types = getFdsCount();
        types = ( types != null ? types : new HashMap<>() );

        // Check for FDS runs.
        if( !runs.isEmpty() ) {
            // Iterate over types.
            for( IcmStatementRun run : runs ) {
                String type = run.getType();
                LocalDateTime start = run.getStart();
                LocalDateTime end = run.getEnd();
                int count = run.getCount();
                int total = types.getOrDefault( type, 0 );
                String status = ( end != null ? "Complete (" + count + ")" : "In Progress (" + count + ")" )
                        + "<br/>Total (" + total + ")";
                String decor = "<span style=\"color:" + ( end != null ? "green" : "red" ) + ";\">{0}</span>";
                // Format FDS details.
                s.append( formatRow( "FDS Status(" + type + ")", start, end, status, decor ) );
            }
        }
        // If no runs, then:
        else {
            s.append( getFdsNoRuns( types ) );
        }

        return s.toString();
    }

    /**
     * Get FDS count.
     * @return Map
     */
    private Map<String, Integer> getFdsCount() {

        // Get FDS settings from properties file.
        ResourceBundle resource = IcmUtil.getInstance().getResource();
        String authendpoint = resource.getString( RunIcmStatements.STATEMENTS_FDSAUTHENDPOINT );
        String endpoint = resource.getString( RunIcmStatements.STATEMENTS_FDSENDPOINT );
        String spaceId = resource.getString( RunIcmStatements.STATEMENTS_FDSSPACEID );
        String clientId = resource.getString( RunIcmStatements.STATEMENTS_FDSCLIENTID );
        String clientSecret = resource.getString( RunIcmStatements.STATEMENTS_FDSCLIENTSECRET );
        String grantType = resource.getString( RunIcmStatements.STATEMENTS_FDSGRANTTYPE );
        String timeout = resource.getString( RunIcmStatements.STATEMENTS_FDSTIMEOUT );
        int t = ( StringUtil.isNumeric( timeout ) ? Integer.parseInt( timeout ) : 500 );
        String count = resource.getString( RunIcmStatements.STATEMENTS_FDSCOUNT );
        int c = ( StringUtil.isValid( count ) && StringUtils.isNumeric( count ) ? Integer.parseInt( count ) : 60 );
        FDSCloudManager fds = new FDSCloudManager( authendpoint, endpoint, clientId, clientSecret, grantType, spaceId, t, c );
        // Create auth token.
        String token = fds.createOAuthToken();

        // Get payout date from config table.
        String payout = dao.getConfig( Config.ICM_STATEMENTS_PAYOUTDATE );
        LocalDate po = LocalDate.parse( payout, DateTimeFormatter.BASIC_ISO_DATE );
        int m = po.getMonthValue();
        int y = po.getYear();
        LOGGER.info( "Month: {}" , m );
        LOGGER.info( "Year:  {}" , y );

        // Get statements from FDS.
        List<FDSDocument> statements = fds.getStatementExternalId( token, null, y, m, null );
        if( statements == null ) {
            LOGGER.warn( "Statement list is null!" );
            return null;
        }
        LOGGER.info( "Statements: {}" , statements.size() );
        Map<String, Integer> types = new HashMap<>();
        if( statements.size() == 0 ) {
            LOGGER.warn( "Statement list is empty!" );
            return types;
        }

        // Create map to count statement types.
        types.put( RunIcmStatements.CSV, 0 );
        types.put( RunIcmStatements.XLS, 0 );
        types.put( RunIcmStatements.PDF, 0 );

        // Iterate over statements.
        for( FDSDocument d : statements ) {
            // Count all the different types.
            String type = ( RunIcmStatements.XLSX.equalsIgnoreCase( d.getType() ) ? RunIcmStatements.XLS : d.getType() );
            types.put( type, types.getOrDefault( type, 0 ) + 1 );
        }

        return types;
    }

    /**
     * @param types Map
     * @return String
     */
    private String getFdsNoRuns( Map<String, Integer> types ) {
        StringBuilder s = new StringBuilder();
        s.append( "<tr valign=\"top\">" );
        s.append( "<td>FDS Status</td>" );
        // If statements in FDS, then:
        if( types.isEmpty() ) {
            s.append( "<td colspan=\"4\" align=\"center\">No FDS statements created for the current month</td>" );
        }
        // Else, display message with FDS statement counts.
        else {
            s.append( "<td colspan=\"4\" align=\"center\">No FDS statement runs during the report window" );
            s.append( "<br/>FDS Totals:" );
            for( Map.Entry<String, Integer> type : types.entrySet() ) {
                s.append( "<br/>" ).append( type.getKey() ).append( ":" ).append( type.getValue() );
            }
            s.append( "</td>" );
        }
        s.append( "</tr>" );
        return s.toString();
    }

    /**
     * Get sync details between start/end date time.
     * @return String
     */
    private String getSyncDetails() {

        IcmSyncRun lastSync = sdao.getLastSyncRun( startDateTime );
        LOGGER.info( "Last sync: {}" , lastSync );

        // Get unsynced data.
        List<IcmSync> unsync = sdao.getSync();
        unsync = ( unsync != null ? unsync : new ArrayList<>() );
        LOGGER.info( "Queued: {}" , unsync.size() );

        Timestamp update = sdao.getLastUpdate();
        String last = ( update != null ? DateUtil.format( update.toLocalDateTime(), DateUtil.FMT_MMDDYYYY_HHMMSSA ) : "unknown" );
        LOGGER.info( "Last: {}" , last );

        if( lastSync == null ) {
            String d = ( !unsync.isEmpty() ? "<span style=\"color:red;\">" +
                    "<br/>Queued: " + unsync.size() + "" +
                    "<br/>Last: " + last + "</span>" : "" );
            return "<tr valign=\"top\">" +
                    "<td>ICM Sync (LPS)</td>" +
                    "<td colspan=\"4\" align=\"center\">No sync done during report window" + d + "</td>" +
                    "</tr>";
        }

        // Iterate over sync data to get start/end dates.
        StringBuilder s = new StringBuilder();

        // Get start and end times.
        LocalDateTime start = ( lastSync.getStart() != null ? lastSync.getStart().toLocalDateTime() : null );
        LocalDateTime end = ( lastSync.getEnd() != null ? lastSync.getEnd().toLocalDateTime() : null );

        // Set status.
        String status = ( end != null ? "Complete (" + lastSync.getCount() + ")" : "Synced:" + lastSync.getCount() + "<br/>Queued:" + unsync.size() + "<br/>Last:" + last );
        String decor = ( end != null ? "<span style=\"color:green;\">{0}</span>" : "<span style=\"color:red;\">{0}</span>" );

        // Format sync details.
        s.append( formatRow( "ICM Sync (LPS)(" + lastSync.getType() + ")", start, end, status, decor ) );

        return s.toString();
    }
}
