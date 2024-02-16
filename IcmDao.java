package com.uhc.optum.icm.webservice;

import com.uhc.optum.dao.MainDao;
import com.uhc.optum.exceptions.DBOperationsException;
import com.uhc.optum.util.DateUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Date;

/**
 * DAO for reading and updating ICM data.
 */
// Suppress "String literals should not be duplicated" warning for Sonar
@java.lang.SuppressWarnings("java:S1192")
public class IcmDao extends MainDao {

    private static final Logger LOGGER = LogManager.getLogger( IcmDao.class );

    public IcmDao( String name ) {
        super( name );
    }

    /**
     * Get all jobs.
     * @return List
     */
    public List<IcmJob> getIcmJobs() {
        List<IcmJob> jobs = new ArrayList<>();
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "SELECT   * " );
        sql.append( "FROM     icm_job " );
        sql.append( "ORDER BY job_name" );
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            ResultSet result = statement.executeQuery();
            while( result.next() ) {
                jobs.add( mapJob( result ) );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get jobs: " + e, sql.toString(), true );
        }
        return jobs;
    }

    /**
     * Get job by name.
     * @param jobId int
     * @return IcmJob
     * @throws DBOperationsException Exception
     */
    public IcmJob getIcmJob( int jobId ) throws DBOperationsException {
        IcmJob job = null;
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "SELECT * " );
        sql.append( "FROM  icm_job " );
        sql.append( "WHERE icm_job_id = ?" );
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            statement.setInt( 1, jobId );
            ResultSet result = statement.executeQuery();
            if( result.next() ) {
                job = mapJob( result );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get job by id [" + jobId + "]: " + e, sql.toString(), true );
        }
        return job;
    }

    /**
     * Get job by name.
     * @param name String
     * @return IcmJob
     * @throws DBOperationsException exception
     */
    public IcmJob getIcmJobByName( String name ) throws DBOperationsException {
        if( name == null ) {
            throw new DBOperationsException( "name is null" );
        }
        IcmJob job = null;
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "SELECT * " );
        sql.append( "FROM   icm_job " );
        sql.append( "WHERE  lower(job_name) = ?" );
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            statement.setString( 1, name );
            ResultSet result = statement.executeQuery();
            if( result.next() ) {
                job = mapJob( result );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get job by name [" + name + "]: " + e, sql.toString(), true );
        }
        return job;
    }

    /**
     * Map job fields.
     * @param result ResultSet
     * @return IcmJob
     * @throws DBOperationsException exception
     */
    private IcmJob mapJob( ResultSet result ) throws DBOperationsException {
        try {
            int id = result.getInt( "icm_job_id" );
            String name = result.getString( "job_name" );
            String desc = result.getString( "job_desc" );
            int wait = result.getInt( "wait_time" );
            int warn = result.getInt( "warn_time" );
            int max = result.getInt( "max_wait_time" );
            Date lastRun = result.getTimestamp( "last_run" );
            boolean active = ( result.getInt( "active" ) == 1 );
            return new IcmJob( id, name, desc, wait, warn, max,
                               ( lastRun != null ? DateUtil.parse( lastRun.getTime() ) : null ),
                               active );
        }
        catch( SQLException e ) {
            throw new DBOperationsException( "Exception in mapJob: " + e, true );
        }
    }

    /**
     * Get job details.
     * @param icmJobId int
     * @throws DBOperationsException exception
     */
    public List<IcmJobDetail> getIcmJobDetails( int icmJobId ) throws DBOperationsException {
        List<IcmJobDetail> details = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append( "SELECT j.job_name, d.* " );
        sql.append( "FROM   icm_job j, icm_job_detail d " );
        sql.append( "WHERE  j.icm_job_id = d.icm_job_id and " );
        sql.append( "j.icm_job_id = ?" );
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            statement.setInt( 1, icmJobId );
            ResultSet result = statement.executeQuery();
            while( result.next() ) {
                int id = result.getInt( "icm_job_id" );
                String name = result.getString( "name" );
                String job = result.getString( "job_name" );
                String value = result.getString( "value" );
                boolean active = ( result.getInt( "active" ) == 1 );
                if( active ) {
                    details.add( new IcmJobDetail( id, job, name, value, active ) );
                }
                else {
                    LOGGER.warn( "JobDetail is not active: " + name + '/' + value );
                }
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get job details[" + icmJobId + "]: " + e, sql.toString(), true );
        }
        return details;
    }

    /**
     * Get job detail.
     * @param icmJobId int
     * @param name     String
     * @return String
     * @throws DBOperationsException exception
     */
    public String getIcmJobDetail( int icmJobId, String name ) throws DBOperationsException {
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "SELECT * " );
        sql.append( "FROM   icm_job_detail " );
        sql.append( "WHERE  icm_job_id = ? and " );
        sql.append( "       name = ?" );
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            statement.setInt( 1, icmJobId );
            statement.setString( 2, name );
            ResultSet result = statement.executeQuery();
            if( result.next() ) {
                String value = result.getString( "value" );
                if( result.getInt( "active" ) == 1 ) {
                    return value;
                }
                else {
                    LOGGER.warn( "JobDetail not active: " + name + '/' + value );
                }
            }
            else {
                LOGGER.error( "JobDetail not found: " + name );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get job detail[" + name + "]: " + e, sql.toString(), true );
        }
        return null;
    }

    /**
     * Get job tasks.
     * @param icmJobId int
     * @return List
     */
    public List<IcmJobTask> getIcmJobTasks( int icmJobId ) {
        List<IcmJobTask> tasks = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append( "SELECT   * " );
        sql.append( "FROM     icm_job_task " );
        sql.append( "WHERE    icm_job_id = ? " );
        sql.append( "ORDER BY task_id" );
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            statement.setInt( 1, icmJobId );
            ResultSet result = statement.executeQuery();
            while( result.next() ) {
                tasks.add( mapJobTask( result ) );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get job tasks[" + icmJobId + "]: " + e, sql.toString(), true );
        }
        return tasks;
    }

    /**
     * Get job task.
     * @param icmJobId int
     * @param taskId   int
     * @return IcmJobTask
     */
    public IcmJobTask getIcmJobTask( int icmJobId, int taskId ) {
        IcmJobTask task = new IcmJobTask();
        StringBuilder sql = new StringBuilder();
        sql.append( "SELECT   * " );
        sql.append( "FROM     icm_job_task " );
        sql.append( "WHERE    icm_job_id = ? and task_id = ? " );
        sql.append( "ORDER BY task_id" );
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            statement.setInt( 1, icmJobId );
            statement.setInt( 2, taskId );
            ResultSet result = statement.executeQuery();
            if( result.next() ) {
                return mapJobTask( result );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get job task[" + icmJobId + "/" + taskId + "]: " + e, sql.toString(), true );
        }
        return task;
    }

    /**
     * Map job task.
     * @param result ResultSet
     * @return IcmJobTask
     */
    private IcmJobTask mapJobTask( ResultSet result ) throws DBOperationsException {
        try {
            int id = result.getInt( "icm_job_id" );
            int taskid = result.getInt( "task_id" );
            String name = result.getString( "task_name" );
            int minutes = result.getInt( "expected_minutes" );
            IcmJobTask t = new IcmJobTask();
            t.setIcmJobId( id );
            t.setTaskId( taskid );
            t.setTaskName( name );
            t.setExpectedMinutes( minutes );
            return t;
        }
        catch( SQLException e ) {
            throw new DBOperationsException( "Exception in mapJobTask: " + e, true );
        }
    }

    /**
     * Update job task.
     * @param icmJobId        int
     * @param taskId          int
     * @param taskName        String
     * @param expectedMinutes int
     */
    public void updateIcmJobTask( int icmJobId, int taskId, String taskName, int expectedMinutes ) {
        String sql = "" +
                "UPDATE icm_job_task " +
                "SET    task_id          = ?, " +
                "       expected_minutes = ? " +
                "WHERE  icm_job_id = ? and task_name = ?";
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql, Statement.RETURN_GENERATED_KEYS ) ) {
            statement.setInt( 1, taskId );
            statement.setInt( 2, expectedMinutes );
            statement.setInt( 3, icmJobId );
            statement.setString( 4, taskName );
            int i = statement.executeUpdate();
            if( i > 0 ) {
                LOGGER.debug( "Task updated: " + i );
            }
            else {
                insertIcmJobTask( icmJobId, taskId, taskName, expectedMinutes );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not update task[" + icmJobId + "]: " + e, sql, true );
        }
    }

    /**
     * Insert job task.
     * @param icmJobId        int
     * @param taskId          int
     * @param taskName        String
     * @param expectedMinutes int
     * @return int
     */
    protected int insertIcmJobTask( int icmJobId, int taskId, String taskName, int expectedMinutes ) {
        ResultSet keys;
        int jobTaskId = 0;
        String sql = "INSERT INTO icm_job_task " +
                "            ( " +
                "            icm_job_id, " +
                "            task_id, " +
                "            task_name, " +
                "            expected_minutes " +
                "            ) " +
                "VALUES      ( " +
                "             ?, " +
                "             ?, " +
                "             ?, " +
                "             ? " +
                "             )";
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql, Statement.RETURN_GENERATED_KEYS ) ) {
            statement.setInt( 1, icmJobId );
            statement.setInt( 2, taskId );
            statement.setString( 3, taskName );
            statement.setInt( 4, expectedMinutes );
            int i = statement.executeUpdate();
            LOGGER.debug( "Task added: " + i );
            keys = statement.getGeneratedKeys();
            if( keys.next() ) {
                jobTaskId = keys.getInt( 1 );
                LOGGER.debug( "New task: " + jobTaskId );
            }
            else {
                LOGGER.error( "No taskId key was generated: " + icmJobId );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not insert task[" + icmJobId + "]: " + e, sql, true );
        }
        return jobTaskId;
    }

    /**
     * Get job history.
     * @return List
     * @throws DBOperationsException exception
     */
    public List<IcmJobHistory> getIcmJobHistory() throws DBOperationsException {
        return getIcmJobHistory( 0, 10 );
    }

    /**
     * Get job history.
     * @param icmJobId int
     * @return List
     * @throws DBOperationsException exception
     */
    public List<IcmJobHistory> getIcmJobHistory( int icmJobId ) throws DBOperationsException {
        return getIcmJobHistory( icmJobId, 0 );
    }

    /**
     * Get job history.
     * @param icmJobId int
     * @param max      int
     * @return List
     * @throws DBOperationsException exception
     */
    public List<IcmJobHistory> getIcmJobHistory( int icmJobId, int max ) throws DBOperationsException {
        List<IcmJobHistory> history = new ArrayList<>();
        String sql = "SELECT   " +
                ( max > 0 ? " TOP " + max + " " : "" ) + " * " +
                "FROM     icm_job_history " +
                ( icmJobId != 0 ? "WHERE    icm_job_id = " + icmJobId : "" ) + " " +
                "ORDER BY start_date desc";
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql ) ) {
            ResultSet result = statement.executeQuery();
            while( result.next() ) {
                history.add( mapJobHistory( result ) );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get job history[" + icmJobId + "]: " + e, sql, true );
        }
        return history;
    }

    /**
     * Get job history.
     * @param start LocalDateTime
     * @param end   LocalDateTime
     * @return List
     * @throws DBOperationsException exception
     */

    //  Created by Akhil

    public LinkedHashMap<String,Integer> getIcmBatchJobsSchedule(LocalDateTime start,LocalDateTime end) throws DBOperationsException{
        LinkedHashMap<String ,Integer>scheduleBatchJobs = new LinkedHashMap<>();

        String sql  = "select j.icm_job_id,ibs.schedule_day,j.job_name,ibs.schedule_time from icm_batch_schedule ibs "+
        "inner join icm_job j on j.icm_job_id=ibs.icm_job_id "
        + "where ibs.active=1 "
        + "and ((ibs.schedule_day in (select DATENAME(WEEKDAY,?)) "
        + "and ibs.schedule_time >=cast(format(DATEPART(hour, ?),'0#') as varchar)+cast(format(DATEPART(minute, ?),'0#') as varchar)) "
        + "OR "
        + "(ibs.schedule_day in (select DATENAME(WEEKDAY,?)) "
        +" And ibs.schedule_time <= 1100)) "
        + "order by CASE ibs.schedule_day "
        + "WHEN 'Sunday' THEN 1 "
        + "WHEN 'Monday' THEN 2 "
        +  "WHEN 'Tuesday' THEN 3 "
        + "WHEN 'Wednesday' THEN 4 "
        + "WHEN 'Thursday' THEN 5 "
        + "WHEN 'Friday' THEN 6 "
        + "ELSE 7  END , "
        + "ibs.schedule_time,j.icm_job_id ";


        LOGGER.debug( "ICM BATCH JOBS SCHEDULE SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql) ) {
            System.out.println(" TimeStamp  >>>> "+ Timestamp.valueOf( start ));
            System.out.println(" TimeStamp  >>>> "+ Timestamp.valueOf( end ));

            statement.setTimestamp( 1, ( start != null ? Timestamp.valueOf( start ) : null ) );
            statement.setTimestamp( 2, ( start != null ? Timestamp.valueOf( start ) : null ) );
            statement.setTimestamp( 3, ( start != null ? Timestamp.valueOf( start ) : null ) );
            statement.setTimestamp( 4, ( end != null ? Timestamp.valueOf( end ) : null ) );


            ResultSet result = statement.executeQuery();
            while( result.next() ) {
                scheduleBatchJobs.put(result.getString("schedule_day")+"#"+result.getString("job_name")+"#"+result.getInt("schedule_time"),result.getInt("icm_job_id"));
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get job history: " + e, sql, true );
        }

        return scheduleBatchJobs;
    }


    public List<IcmJobHistory> getIcmCompletedJob( LocalDateTime start, LocalDateTime end ,int icmJobId) throws DBOperationsException {
        List<IcmJobHistory> completedIcmJob = new ArrayList<>();
        String sql = "SELECT   *  "
        + " FROM   icm_job_history  "
        + "WHERE    start_date >= ? and end_date <= ?"
        + " and icm_job_id = ? "
        + "ORDER BY start_date";

        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql) ) {
            statement.setTimestamp( 1, ( start != null ? Timestamp.valueOf( start ) : null ) );
            statement.setTimestamp( 2, ( end != null ? Timestamp.valueOf( end ) : null ) );
            statement.setInt(3,icmJobId);
            ResultSet result = statement.executeQuery();


            while( result.next() ) {
                completedIcmJob.add(mapJobHistory( result ));
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get job history: " + e, sql, true );
        }
        return completedIcmJob;
    }

    public List<IcmJobHistory> getIcmCompletedJobScheduledMorethanOnce( LocalDateTime start, LocalDateTime end ,int icmJobId,String mapKey,int jobOccurence) throws DBOperationsException ,NumberFormatException{

        String[] jobScheduleInfo = mapKey.split("#");

        String scheduleDay = jobScheduleInfo[0];
        int scheduleTime = Integer.parseInt(jobScheduleInfo[2]);


        LocalDateTime  starttmsmp = null;
        LocalDateTime  endtmsp = null;
        String jobStream= null;

            jobStream = getJobStream(scheduleDay,icmJobId,scheduleTime) ;
             if(jobStream !=null){
                 LocalDateTime[] jobStreamStartAndEndTime = getJobStreamStartAndEndTime(jobStream,icmJobId,start,end,scheduleTime,scheduleDay);
                 starttmsmp  = jobStreamStartAndEndTime[0];
                 endtmsp = jobStreamStartAndEndTime[1];
             }


        List<IcmJobHistory> completedIcmJob = new ArrayList<>();
        String sql = "SELECT   *  "
                + " FROM   icm_job_history  "
                + "WHERE    start_date >= ? and end_date <= ?"
                + " and icm_job_id = ? "
                + "ORDER BY start_date";

        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql) ) {
            if(jobOccurence>1 && jobStream != null ){
                statement.setTimestamp( 1, ( starttmsmp != null ? Timestamp.valueOf( starttmsmp ) : null ) );
                statement.setTimestamp( 2, ( endtmsp != null ? Timestamp.valueOf( endtmsp ) : null ) );

            }else{
                statement.setTimestamp( 1, ( start != null ? Timestamp.valueOf( start ) : null ) );
                statement.setTimestamp( 2, ( end != null ? Timestamp.valueOf( end ) : null ) );

            }

            statement.setInt(3,icmJobId);
            ResultSet result = statement.executeQuery();
            while( result.next() ) {
                   completedIcmJob.add(mapJobHistory(result));
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get job history: " + e, sql, true );
        }
        return completedIcmJob;
    }

    public LocalDateTime[] getJobStreamStartAndEndTime(String jobStream,int icmJobId,LocalDateTime start,LocalDateTime end ,int scheduleTime,String scheduleDay){
        LocalDateTime startTime = null;
        LocalDateTime endTime = null;
        LocalDateTime maxStartTime = null;
        LocalDateTime [] jobStreamSchedule = {};
        String sql = "SELECT CASE WHEN MIN(start_date) is null THEN DATEADD(YEAR, -1, GETDATE()) ELSE MIN(start_date) END AS start_date " +
                " ,CASE WHEN MAX(end_date) is null THEN DATEADD(YEAR, -1, GETDATE()) ELSE MAX(end_date) END AS end_date "
                +",CASE WHEN MAX(start_date)  is null THEN DATEADD(YEAR, -1, GETDATE()) ELSE MAX(start_date) END AS max_start_date "
                +" FROM icm_job_history "
                +" WHERE icm_job_id IN "
                +" (SELECT icm_job_id FROM icm_batch_schedule "
                + " WHERE job_stream = ?  AND schedule_day = ? AND icm_job_id <> ?)"
                + " AND start_date >= ? AND (end_date <= ?  OR end_date is null)  ";


        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql ) ) {
            statement.setString(1,jobStream);
            LOGGER.info("The Value of Completed Job Stream : {}",jobStream);

            statement.setString(2,scheduleDay);

            statement.setInt( 3, icmJobId );


            statement.setTimestamp( 4, ( start != null ? Timestamp.valueOf( start ) :null) );
            statement.setTimestamp( 5, ( end != null ? Timestamp.valueOf( end ) : null) );

            ResultSet result = statement.executeQuery();
            while( result.next() ) {

                startTime = result.getTimestamp("start_date").toLocalDateTime();
                LOGGER.info("Completed JobStream Start Time == >>>> :   {}", startTime);
                endTime = result.getTimestamp("end_date").toLocalDateTime();
                LOGGER.info("Completed JobStream End Time == >>>> :   {}", endTime);

                maxStartTime = result.getTimestamp("max_start_date").toLocalDateTime();

                LOGGER.info("Max Start Time ==  >> {}", maxStartTime);

                if (maxStartTime.compareTo(endTime) > 0) {
                    endTime = maxStartTime;
                }

                int recordCount = checkCurrentJobEntryIsNull(icmJobId, start, scheduleTime, scheduleDay, jobStream);
                LOGGER.info("The Value of Record count : {} ", recordCount);

                jobStreamSchedule = jobStreamScheduleStartTimeandEndTime(recordCount, startTime, start, endTime, end,jobStream);
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get in Job Stream Start and End time [" + icmJobId + "]: " + e, sql, true );
        }

        return jobStreamSchedule;
    }

    private LocalDateTime[] jobStreamScheduleStartTimeandEndTime(int recordCount, LocalDateTime startTime, LocalDateTime start, LocalDateTime endTime, LocalDateTime end,String jobStream) {
        LocalDateTime [] jobStreamSchedule = {};
            if(recordCount >0){
                jobStreamSchedule = new LocalDateTime[]{(startTime!=null)?startTime:start,(endTime!=null)?endTime:end};
            }else{
                jobStreamSchedule = new LocalDateTime[]{startTime,endTime};
            }

            if(checkstartTimeandEndTime(startTime,start,endTime)){
                jobStreamSchedule = new LocalDateTime[]{};
                jobStreamSchedule =  getJobStreamScheduleStartAndEndTime(jobStream,start,end);                }
        return jobStreamSchedule;


        }

    private boolean checkstartTimeandEndTime(LocalDateTime startTime, LocalDateTime start, LocalDateTime endTime) {
        if(startTime.isBefore(start)  || endTime.isBefore(start) ) return true;
        return false;
    }

    public int checkCurrentJobEntryIsNull(int icmJobId, LocalDateTime start ,int scheduleTime,String scheduleDay,String jobStream) {

        int records = 0;
        String query = "select count(jh.icm_job_id) as count from icm_batch_schedule ibs inner join icm_job_history jh  on "
                +" ibs.icm_job_id = jh.icm_job_id "
                +" where jh.start_date  >= ? AND ibs.schedule_time < ? "
                +" and ibs.job_stream <> ? "
                +" and ibs.schedule_day = ? ";

        LOGGER.debug( "SQL: " + query );
        try( PreparedStatement statement = getConnection().prepareStatement( query ) ) {

            statement.setTimestamp( 1, ( start != null ? Timestamp.valueOf( start ) : null ) );
            statement.setInt(2,scheduleTime);

            statement.setString( 3, jobStream );

            statement.setString( 4, scheduleDay );


            ResultSet result = statement.executeQuery();
            while( result.next() ) {
                records = result.getInt("count") ;
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + query );
            throw new DBOperationsException( "Could not get in process job[" + icmJobId + "]: " + e, query, true );
        }
        return records;

    }

    public LocalDateTime[] getJobStreamScheduleStartAndEndTime(String jobStream, LocalDateTime start, LocalDateTime end){
        int minscheduletime = 0,maxscheduletime = 0;
        String jsminscheduletime = null;
        String jsmaxscheduletime = null;
        LocalDateTime startdateTime = null;
        LocalDateTime enddateTime = null;
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.nnn");

        String sql = "SELECT min(schedule_time) as minscheduletime,max(schedule_time) as maxscheduletime ,"
                    + " (CAST(ISNULL(format(min(schedule_time),'00:00'),0) AS VARCHAR)+':00.000') as jsminscheduletime, "
                    + " (CAST(ISNULL(format(max(schedule_time),'00:00'),0) AS VARCHAR)+':00.000') as jsmaxscheduletime "
                    + " FROM icm_batch_schedule  WHERE job_stream = ? ";

        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql) ) {

            statement.setString(1,jobStream);
            ResultSet result = statement.executeQuery();
            while( result.next() ) {
                 minscheduletime = result.getInt("minscheduletime");
                 maxscheduletime = result.getInt("maxscheduletime");
                 jsminscheduletime = result.getString("jsminscheduletime");
                 jsmaxscheduletime = result.getString("jsmaxscheduletime");
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get job history: " + e, sql, true );
        }

        if(minscheduletime >= 1700  ){
            String startDate = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ENGLISH).format(start);
            startdateTime = LocalDateTime.parse(startDate+' '+jsminscheduletime, formatter);

        }else{
            String startDate = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ENGLISH).format(end);
            startdateTime = LocalDateTime.parse(startDate+' '+jsminscheduletime, formatter);
        }

        if(maxscheduletime >= 1700  ){
            String endDate = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ENGLISH).format(start);
            enddateTime = LocalDateTime.parse(endDate+' '+jsmaxscheduletime, formatter);
        }else{
            String endDate = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ENGLISH).format(end);
            enddateTime = LocalDateTime.parse(endDate+' '+jsmaxscheduletime, formatter);

        }

     return new LocalDateTime[]{startdateTime,enddateTime};

    }
//     Method to obtain the jobStream name for the job
    public String getJobStream(String scheduleDay,int icmJobId,int scheduleTime){
        String jobStream= null;
        String query = "SELECT job_stream FROM icm_batch_schedule "
                +" WHERE schedule_day = ? "
                +" AND icm_job_id  = ? "
                +" AND schedule_time = ? ";

        LOGGER.debug( "SQL: " + query );
        try( PreparedStatement statement = getConnection().prepareStatement( query ) ) {
            statement.setString(1,scheduleDay);
            if( icmJobId != 0 ) {
                statement.setInt( 2, icmJobId );
            }
            statement.setInt(3,scheduleTime);


            ResultSet result = statement.executeQuery();
            while( result.next() ) {
                 jobStream = result.getString("job_stream") ;
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + query );
            throw new DBOperationsException( "Could not get the Job Stream for job id :  [" + icmJobId + "]: " + e, query, true );
        }
        return jobStream;
    }


    public IcmJobHistory getIcmInprogressJob( int icmJobId) throws DBOperationsException {
        IcmJobHistory inprogressJob = null;
        String sql =  "SELECT   * "
                    + "FROM icm_job_history "
                    +  "WHERE " + (icmJobId != 0 ? "icm_job_id = ? and " : "")
                    +  "start_date is not null and "
                    +  "end_date is null "
                    +  "ORDER BY start_date" ;

        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql ) ) {
            if( icmJobId != 0 ) {
                statement.setInt( 1, icmJobId );
            }
            ResultSet result = statement.executeQuery();
            while( result.next() ) {
               inprogressJob =  mapJobHistory( result ) ;
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get in process job[" + icmJobId + "]: " + e, sql, true );
        }
        return inprogressJob;
    }

    public IcmJobHistory getIcmInprogressJobScheduledMorethanOnce(LocalDateTime start ,int icmJobId,String mapKey) throws  NumberFormatException,DBOperationsException{
        IcmJobHistory inprogressJob = null;
        String jobStream = null;

        String[] arrayOfString = mapKey.split("#");

        String scheduleDay = arrayOfString[0];
        int scheduleTime = Integer.parseInt(arrayOfString[2]);


        jobStream = getJobStream(scheduleDay,icmJobId,scheduleTime) ;

        LocalDateTime jobStreamStarttTime= getJobStreamStarttTime(jobStream,start,scheduleDay,icmJobId);

        LOGGER.info("InProgress JobStream : {} and  JobStream Start Time : {}",jobStream,jobStreamStarttTime);

        String sql =  "SELECT * FROM icm_job_history jh INNER JOIN "
                + " icm_batch_schedule ibs "
                +" on jh.icm_job_id = ibs.icm_job_id "
                +" where ibs.job_stream = ? "
                +" and jh.start_date > ? "
                +" and jh.end_date is null "
               + " and ibs.icm_job_id = ? " ;

        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql ) ) {
            if(jobStream != null && jobStream.length() > 0) {
                statement.setString(1, jobStream);
            }
            statement.setTimestamp( 2, ( jobStreamStarttTime != null ? Timestamp.valueOf( jobStreamStarttTime ) : null ) );

            if( icmJobId != 0 ) {
                statement.setInt( 3, icmJobId );
            }

            ResultSet result = statement.executeQuery();
            while( result.next() ) {
                inprogressJob =  mapJobHistory( result ) ;
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get in process job [" + icmJobId + "]: for Job Stream [" +jobStream+ "]"  + e, sql, true );
        }
        return inprogressJob;

    }

    public LocalDateTime getJobStreamStarttTime(String jobStream,LocalDateTime start ,String scheduleDay, int icmJobId){
        LocalDateTime jobStreamStartTime = null;

        String sql = "SELECT CASE WHEN MIN(start_date) is null THEN  GETDATE() ELSE MIN(start_date) End AS start_date  "
                + "  FROM icm_job_history  WHERE icm_job_id IN "
                + "  (select icm_job_id from  icm_batch_schedule where job_stream = ? and schedule_day = ? and icm_job_id <> ?)"
                + "  AND start_date >= ?";


        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql ) ) {

            if(jobStream != null && jobStream.length() > 0) {
                statement.setString(1, jobStream);
            }
            statement.setString(2,scheduleDay);
            statement.setInt(3,icmJobId);
            statement.setTimestamp( 4, ( start != null ? Timestamp.valueOf( start ) : null ) );


            ResultSet result = statement.executeQuery();
            while( result.next() ) {
                jobStreamStartTime =  result.getTimestamp("start_date").toLocalDateTime() ;
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get in JobStream start Time  for Job Stream [" + jobStream + "]: " + e, sql, true );
        }

        return jobStreamStartTime;

    }

    public boolean getLastScheduledJobStatusIpsAndAcra(LocalDateTime start,LocalDateTime end){
        String sql =  " select * from icm_job_history jh where icm_job_id in (select top 1 icm_job_id  from icm_batch_schedule  ibs "
                + " where ibs.schedule_day =(SELECT DATENAME(dw,GETDATE())) "
                + " and ibs.schedule_time < 1100  "
                + " order by ibs.schedule_time desc) "
                + " and jh.start_date >= ? and jh.end_date <=  ? "   ;

        LOGGER.debug( "SQL: " + sql );
        LOGGER.info("Getting the last scheduled job status ");
        try(PreparedStatement statement = getConnection().prepareStatement(sql)){
            statement.setTimestamp( 1, ( start != null ? Timestamp.valueOf( start ) : null ) );
            statement.setTimestamp( 2, ( end != null ? Timestamp.valueOf( end ) : null ) );
            ResultSet result = statement.executeQuery();


            if(result.isBeforeFirst()){
                return true;

            }


        } catch (Exception e){
            LOGGER.error("SQL:"+sql);
            throw new DBOperationsException( "Could get the last job Scheduled : FOR Acra and IPS  " + e, sql, true );

        }


        return false;
    }
    public boolean getLastScheduledJobStatus(LocalDateTime start,LocalDateTime end){

          String sql  = "select * from icm_job_history jh where icm_job_id in (select  top 1 icm_job_id from icm_batch_schedule  ibs "
                  +  " where ibs.schedule_day =(SELECT DATENAME(dw,GETDATE()))  "
                  +  " and ibs.schedule_time < 1100 and "
                  +  " ibs.icm_job_id not in(select icm_job_id from icm_job where job_name like '%ips_transactions' or job_name like '%acra_debtloader') "
                  +  "order by ibs.schedule_time desc)  and jh.start_date >= ?  and jh.end_date <=  ?  ";

        LOGGER.debug( "SQL: " + sql );
        LOGGER.info("Getting the last scheduled job status ");
        try(PreparedStatement statement = getConnection().prepareStatement(sql)){
            statement.setTimestamp( 1, ( start != null ? Timestamp.valueOf( start ) : null ) );
            statement.setTimestamp( 2, ( end != null ? Timestamp.valueOf( end ) : null ) );
            ResultSet result = statement.executeQuery();


            if(result.isBeforeFirst()){
                return true;
            }

        } catch (Exception e){
            LOGGER.error("SQL:"+sql);
            throw new DBOperationsException( "Could get the last job Scheduled : " + e, sql, true );

        }

        return false;

    }

    public List<IcmJobHistory> getIcmJobHistory( LocalDateTime start, LocalDateTime end ) throws DBOperationsException {
        List<IcmJobHistory> history = new ArrayList<>();
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "SELECT   * " );
        sql.append( "FROM     icm_job_history " );
        sql.append( "WHERE    start_date >= ? and end_date <= ?" );
        sql.append( "ORDER BY start_date" );
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            statement.setTimestamp( 1, ( start != null ? Timestamp.valueOf( start ) : null ) );
            statement.setTimestamp( 2, ( end != null ? Timestamp.valueOf( end ) : null ) );
            ResultSet result = statement.executeQuery();
            while( result.next() ) {
                history.add( mapJobHistory( result ) );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get job history: " + e, sql.toString(), true );
        }
        return history;
    }


    public String getRunCycleForOnDemandJobs(int icmJobId,String scheduleDay,int scheduleTime) throws DBOperationsException {
        String runcycle = "";
        String sql = "SELECT runcycle "
                  +"FROM icm_batch_schedule "
                  +"WHERE icm_job_id = ?"
                  +" AND schedule_day = ? "
                  +" AND schedule_time = ? ";

        LOGGER.debug("SQL :",sql);

        try( PreparedStatement statement = getConnection().prepareStatement( sql ) ) {
            statement.setInt(1,icmJobId);
            statement.setString(2,scheduleDay);
            statement.setInt(3,scheduleTime);
            ResultSet result = statement.executeQuery();
            while( result.next() ) {
                runcycle = result.getString("runcycle");
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get RunCycle : " + e, sql, true );
        }


        return runcycle;
    }



    /**
     * Get job history.
     * @param runListNo String
     * @return IcmJobHistory
     * @throws DBOperationsException exception
     */
    public IcmJobHistory getIcmJobHistory( String runListNo ) throws DBOperationsException {
        IcmJobHistory history = null;
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "SELECT * " );
        sql.append( "FROM   icm_job_history " );
        sql.append( "WHERE  run_list_no = ?" );
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            statement.setString( 1, runListNo );
            ResultSet result = statement.executeQuery();
            if( result.next() ) {
                history = mapJobHistory( result );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get job history[" + runListNo + "]: " + e, sql.toString(), true );
        }
        return history;
    }

    /**
     * Get last completed payout date.
     * @return LocalDate
     */
    public LocalDate getLastCompletedPayout() {
        LocalDate payout = null;
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "SELECT max(h.start_date) " );
        sql.append( "FROM icm_job j " );
        sql.append( "INNER JOIN icm_job_history h on j.icm_job_id = h.icm_job_id " );
        sql.append( "WHERE j.job_name like '%payout%' and " );
        sql.append( "j.job_name not like '%final%' and " );
        sql.append( "h.end_date is not null" );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            ResultSet result = statement.executeQuery();
            LOGGER.debug( "SQL: " + sql );
            if( result.next() ) {
                Timestamp ts = result.getTimestamp( 1 );
                if( ts != null ) {
                    // Convert from timestamp to localdate.
                    payout = ts.toLocalDateTime().toLocalDate();
                }
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get last completed payout date: " + e, sql.toString(), true );
        }
        return payout;
    }

    /**
     * Get all in process jobs.
     * @return List
     * @throws DBOperationsException exception
     */
    public List<IcmJobHistory> getInProcessJobs() throws DBOperationsException {
        return getInProcessJob( 0 );
    }

    /**
     * Get in process jobs for job id.
     * @param icmJobId int
     * @return List
     * @throws DBOperationsException exception
     */
    public List<IcmJobHistory> getInProcessJob( int icmJobId ) throws DBOperationsException {
        List<IcmJobHistory> history = new ArrayList<>();
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "SELECT   * " );
        sql.append( "FROM icm_job_history " );
        sql.append( "WHERE " ).append( icmJobId != 0 ? "icm_job_id = ? and " : "" );
        sql.append( "start_date is not null and " );
        sql.append( "end_date is null " );
        sql.append( "ORDER BY start_date" );
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            if( icmJobId != 0 ) {
                statement.setInt( 1, icmJobId );
            }
            ResultSet result = statement.executeQuery();
            while( result.next() ) {
                history.add( mapJobHistory( result ) );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get in process job[" + icmJobId + "]: " + e, sql.toString(), true );
        }
        return history;
    }

    /**
     * Map job history fields.
     * @param result ResultSet
     * @return IcmJobHistory
     * @throws DBOperationsException exception
     */
    private IcmJobHistory mapJobHistory( ResultSet result ) throws DBOperationsException {
        try {
            int id = result.getInt( "icm_job_history_id" );
            int icmJobId = result.getInt( "icm_job_id" );
            Date start = result.getTimestamp( "start_date" );
            Date end = result.getTimestamp( "end_date" );
            boolean success = ( result.getInt( "success" ) == 1 );
            String message = result.getString( "message" );
            String runListNo = result.getString( "run_list_no" );
            String filename = result.getString( "filename" );
            return new IcmJobHistory( id, icmJobId,
                                      ( start != null ? DateUtil.parse( start.getTime() ) : null ),
                                      ( end != null ? DateUtil.parse( end.getTime() ) : null ),
                                      success, message, runListNo, filename );
        }
        catch( Exception e ) {
            throw new DBOperationsException( "Exception in mapJobHistory: " + e, true );
        }
    }




    /**
     * Get job history tasks.
     * @param icmJobHistoryId int
     * @return List
     */
    public List<IcmJobTaskHistory> getIcmJobTaskHistory( int icmJobHistoryId ) {

        List<IcmJobTaskHistory> list = new ArrayList<>();

        StringBuilder sql = new StringBuilder( 400 )
                .append( "SELECT   * " )
                .append( "FROM     icm_job_task_history " )
                .append( "WHERE    icm_job_history_id = ? " )
                .append( "ORDER BY start_date" );
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            statement.setInt( 1, icmJobHistoryId );
            ResultSet result = statement.executeQuery();
            while( result.next() ) {
                list.add( mapJobTaskHistory( result ) );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get job task history[" + icmJobHistoryId + "]: " + e, sql.toString(), true );
        }

        return list;
    }

    /**
     * Map job task history.
     * @param result ResultSet
     * @return IcmJobTaskHistory
     */
    private IcmJobTaskHistory mapJobTaskHistory( ResultSet result ) {
        try {
            int id = result.getInt( "icm_job_task_history_id" );
            int icmJobHistoryId = result.getInt( "icm_job_history_id" );
            int icmJobTaskId = result.getInt( "icm_job_task_id" );
            int icmJobId = result.getInt( "icm_job_id" );
            Date start = result.getTimestamp( "start_date" );
            Date end = result.getTimestamp( "end_date" );
            String message = result.getString( "message" );
            return new IcmJobTaskHistory( id, icmJobHistoryId, icmJobTaskId, icmJobId,
                                          ( start != null ? DateUtil.parse( start.getTime() ) : null ),
                                          ( end != null ? DateUtil.parse( end.getTime() ) : null ),
                                          message );
        }
        catch( Exception e ) {
            throw new DBOperationsException( "Exception in mapJobHistory: " + e, true );
        }
    }

    /**
     * Update job task history task.
     * @param icmJobHistoryId int
     * @param icmJobTaskId    int
     * @param icmJobId        int
     * @param startDate       LocalDateTime
     * @param endDate         LocalDateTime
     * @param message         String
     */
    public void updateIcmJobTaskHistory( int icmJobHistoryId, int icmJobTaskId, int icmJobId, LocalDateTime startDate, LocalDateTime endDate, String message ) {
        String sql = "" +
                "UPDATE icm_job_task_history " +
                "SET    start_date = ?, " +
                "       end_date   = ?, " +
                "       message    = ? " +
                "WHERE  icm_job_history_id = ? and icm_job_task_id = ?";
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql, Statement.RETURN_GENERATED_KEYS ) ) {
            statement.setTimestamp( 1, Timestamp.valueOf( startDate ) );
            statement.setTimestamp( 2, ( endDate != null ? Timestamp.valueOf( endDate ) : null ) );
            statement.setString( 3, message );
            statement.setInt( 4, icmJobHistoryId );
            statement.setInt( 5, icmJobTaskId );
            int i = statement.executeUpdate();
            if( i > 0 ) {
                LOGGER.debug( "Task history updated: " + i );
            }
            else {
                insertIcmJobTaskHistory( icmJobHistoryId, icmJobTaskId, icmJobId, startDate, endDate, message );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not update job task history[" + icmJobId + "]: " + e, sql, true );
        }
    }

    /**
     * Insert job task history.
     * @param icmJobHistoryId int
     * @param icmJobTaskId    int
     * @param icmJobId        int
     * @param startDate       LocalDateTime
     * @param endDate         LocalDateTime
     * @param message         String
     * @return int
     */
    protected int insertIcmJobTaskHistory( int icmJobHistoryId, int icmJobTaskId, int icmJobId, LocalDateTime startDate, LocalDateTime endDate, String message ) {
        ResultSet keys;
        int jobTaskHistoryId = 0;
        String sql = "INSERT INTO icm_job_task_history " +
                "            ( " +
                "            icm_job_history_id, " +
                "            icm_job_task_id, " +
                "            icm_job_id, " +
                "            start_date, " +
                "            end_date, " +
                "            message " +
                "            ) " +
                "VALUES      ( " +
                "             ?, " +
                "             ?, " +
                "             ?, " +
                "             ?, " +
                "             ?, " +
                "             ? " +
                "             )";
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql, Statement.RETURN_GENERATED_KEYS ) ) {
            statement.setInt( 1, icmJobHistoryId );
            statement.setInt( 2, icmJobTaskId );
            statement.setInt( 3, icmJobId );
            statement.setTimestamp( 4, Timestamp.valueOf( startDate ) );
            statement.setTimestamp( 5, ( endDate != null ? Timestamp.valueOf( endDate ) : null ) );
            statement.setString( 6, message );
            int i = statement.executeUpdate();
            LOGGER.debug( "Task history added: " + i );
            keys = statement.getGeneratedKeys();
            if( keys.next() ) {
                jobTaskHistoryId = keys.getInt( 1 );
                LOGGER.debug( "New task history: " + jobTaskHistoryId );
            }
            else {
                LOGGER.error( "No task history id key was generated: " + icmJobId );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not insert job task history[" + icmJobId + "]: " + e, sql, true );
        }
        return jobTaskHistoryId;
    }

    /**
     * Update job.
     * @param job IcmJob
     */
    public void updateJob( IcmJob job ) {
        if( job == null ) {
            throw new DBOperationsException( "Could not update job!" );
        }
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "UPDATE icm_job " );
        sql.append( "SET    job_name      = ?, " );
        sql.append( "       job_desc      = ?, " );
        sql.append( "       wait_time     = ?, " );
        sql.append( "       warn_time     = ?, " );
        sql.append( "       max_wait_time = ?, " );
        sql.append( "       last_run      = ?, " );
        sql.append( "       active        = ? " );
        sql.append( "WHERE  icm_job_id    = ?" );
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            statement.setString( 1, job.getName() );
            statement.setString( 2, job.getDesc() );
            statement.setInt( 3, job.getWaitSeconds() );
            statement.setInt( 4, job.getWarnMinutes() );
            statement.setInt( 5, job.getMaxMinutes() );
            statement.setTimestamp( 6, ( job.getLastRun() != null ? Timestamp.valueOf( job.getLastRun() ) : null ) );
            statement.setInt( 7, job.isActive() ? 1 : 0 );
            statement.setInt( 8, job.getId() );
            statement.executeUpdate();
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not update job: " + e, sql.toString(), true );
        }
    }

    /**
     * Update last run for job.
     * @param icmJobId int
     * @param lastRun  LocalDateTime
     */
    public void updateLastRun( int icmJobId, LocalDateTime lastRun ) throws DBOperationsException {
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "UPDATE icm_job " );
        sql.append( "SET    last_run = ? " );
        sql.append( "WHERE  icm_job_id = ?" );
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            statement.setTimestamp( 1, ( lastRun != null ? Timestamp.valueOf( lastRun ) : null ) );
            statement.setInt( 2, icmJobId );
            statement.executeUpdate();
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not update last run for job[" + icmJobId + "]: " + e, sql.toString(), true );
        }
    }

    /**
     * Insert job history.
     * @param icmJobId  int
     * @param start     LocalDateTime
     * @param end       LocalDateTime
     * @param success   boolean
     * @param message   String
     * @param runListNo String
     * @param filename  String
     * @return int
     */
    protected int insertIcmJobHistory( int icmJobId, LocalDateTime start, LocalDateTime end, boolean success, String message, String runListNo, String filename ) throws DBOperationsException {
        int historyId = 0;
        String sql = "INSERT INTO icm_job_history " +
                "            ( " +
                "            icm_job_id, " +
                "            start_date, " +
                "            end_date, " +
                "            success, " +
                "            message, " +
                "            run_list_no, " +
                "            filename " +
                "            ) " +
                "VALUES      ( " +
                "             ?, " +
                "             ?, " +
                "             ?, " +
                "             ?, " +
                "             ?, " +
                "             ?, " +
                "             ? " +
                "             )";
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql, Statement.RETURN_GENERATED_KEYS ) ) {
            statement.setInt( 1, icmJobId );
            statement.setTimestamp( 2, ( start != null ? Timestamp.valueOf( start ) : null ) );
            statement.setTimestamp( 3, ( end != null ? Timestamp.valueOf( end ) : null ) );
            statement.setInt( 4, ( success ? 1 : 0 ) );
            statement.setString( 5, message );
            statement.setString( 6, runListNo );
            statement.setString( 7, filename );
            int i = statement.executeUpdate();
            LOGGER.debug( "History added: " + i );
            ResultSet keys = statement.getGeneratedKeys();
            if( keys.next() ) {
                historyId = keys.getInt( 1 );
                LOGGER.debug( "New historyId: " + historyId );
            }
            else {
                LOGGER.error( "No historyId key was generated: " + icmJobId );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not insert job history[" + icmJobId + "]: " + e, sql, true );
        }
        return historyId;
    }

    /**
     * Insert job history.
     * @param history IcmJobHistory
     * @return int
     */
    protected int insertIcmJobHistory( IcmJobHistory history ) throws DBOperationsException {
        int id = insertIcmJobHistory( history.getJobId(), history.getStart(), history.getEnd(),
                                      history.isSuccess(), history.getMessage(), history.getRunListNo(), history.getFilename() );
        history.setId( id );
        return id;
    }

    /**
     * Update job history.
     * @param history IcmJobHistory
     */
    public void updateIcmJobHistory( IcmJobHistory history ) throws DBOperationsException {
        if( history.getStart() == null ) {
            history.setStart( LocalDateTime.now() );
        }
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "UPDATE icm_job_history " );
        sql.append( "SET    start_date  =  ?, " );
        sql.append( "       end_date    =  ?, " );
        sql.append( "       success     =  ?, " );
        sql.append( "       message     =  ?, " );
        sql.append( "       run_list_no =  ?, " );
        sql.append( "       filename    =  ? " );
        sql.append( "WHERE  icm_job_history_id = ?" );
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            statement.setTimestamp( 1, ( history.getStart() != null ? Timestamp.valueOf( history.getStart() ) : null ) );
            statement.setTimestamp( 2, ( history.getEnd() != null ? Timestamp.valueOf( history.getEnd() ) : null ) );
            statement.setInt( 3, history.isSuccess() ? 1 : 0 );
            statement.setString( 4, history.getMessage() );
            statement.setString( 5, history.getRunListNo() );
            statement.setString( 6, history.getFilename() );
            statement.setInt( 7, history.getId() );
            int i = statement.executeUpdate();
            LOGGER.debug( "History updated: " + i );
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not update job history: " + e, sql.toString(), true );
        }
    }

    /**
     * Get statement producer.
     * @param partyId String
     * @return IcmProducer
     */
    public IcmProducer getStatementProducer( String partyId ) {
        IcmProducer producer = null;
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "SELECT    p.party_id, p.large, p.retrieval, p.push_eqc " );
        sql.append( "FROM      icm_statement_producer p " );
        sql.append( "LEFT JOIN icm_statement s on s.party_id = p.party_id " );
        sql.append( "WHERE     p.party_id = ?" );
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            statement.setString( 1, partyId );
            ResultSet result = statement.executeQuery();
            if( result.next() ) {
                producer = mapProducer( result );
            }
            else {
                LOGGER.warn( "Could not find party id: " + partyId );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get statement producer: " + e, sql.toString(), true );
        }

        return producer;
    }

    /**
     * Map producer.
     * @param result ResultSet
     * @return IcmProducer
     * @throws DBOperationsException Exception
     */
    private IcmProducer mapProducer( ResultSet result ) throws DBOperationsException {
        IcmProducer producer;
        try {
            String pid = result.getString( "party_id" );
            producer = new IcmProducer( pid );
            producer.setLarge( result.getInt( "large" ) == 1 );
            int retrieval = result.getInt( "retrieval" );
            retrieval = ( retrieval == IcmProducer.PROCESS_REPORT || retrieval == IcmProducer.EXTRACT ? retrieval : IcmProducer.PROCESS_REPORT );
            producer.setRetrieval( retrieval );
            producer.setPushEcg( result.getInt( "push_eqc" ) == 1 );
        }
        catch( Exception e ) {
            throw new DBOperationsException( "Exception in mapProducer: " + e, true );
        }
        return producer;
    }

    /**
     * Get statement producers with duplicate party ids.
     * @return List
     */
    public List<IcmProducer> getDuplicatePartyIds() throws DBOperationsException {
        List<IcmProducer> list = new ArrayList<>();
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "select party_id, count(*) dupes " );
        sql.append( "from icm_statement_producer " );
        sql.append( "group by party_id " );
        sql.append( "having count(*) > 1 " );
        sql.append( "order by 1 desc" );
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            ResultSet result = statement.executeQuery();
            while( result.next() ) {
                String pid = result.getString( 1 );
                LOGGER.warn( pid );
                list.add( new IcmProducer( pid ) );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get duplicate party ids: " + e, sql.toString(), true );
        }
        return list;
    }

    /**
     * Get statement producers with specified large value.
     * @param large int
     * @return List
     */
    public List<IcmProducer> getStatementProducers( int large ) throws DBOperationsException {
        List<IcmProducer> list = new ArrayList<>();
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "SELECT   p.party_id, p.large, p.retrieval, p.push_eqc " );
        sql.append( "FROM     icm_statement_producer p " );
        sql.append( "WHERE    p.large = ? " );
        sql.append( "ORDER BY p.party_id" );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            statement.setInt( 1, large );
            ResultSet result = statement.executeQuery();
            LOGGER.debug( "SQL: " + sql );
            while( result.next() ) {
                list.add( mapProducer( result ) );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get statement producers: " + e, sql.toString(), true );
        }
        return list;
    }

    /**
     * Get statement producers.
     * @return Map
     * @throws DBOperationsException Exception
     */
    public Map<String, IcmProducer> getStatementProducers() throws DBOperationsException {
        Map<String, IcmProducer> map = new HashMap<>();
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "SELECT   p.party_id, p.large, p.retrieval, p.push_eqc " );
        sql.append( "FROM     icm_statement_producer p " );
        sql.append( "ORDER BY p.party_id" );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            ResultSet result = statement.executeQuery();
            LOGGER.debug( "SQL: " + sql );
            while( result.next() ) {
                IcmProducer p = mapProducer( result );
                map.put( p.getPartyId(), p );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get statement producers: " + e, sql.toString(), true );
        }
        return map;
    }

    /**
     * Add new producers to table.
     * @return long
     * @throws DBOperationsException Exception
     */
    public long addStatementProducers() throws DBOperationsException {
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "select party_id " );
        sql.append( "from   icm_statement " );
        sql.append( "where  party_id not in ( select party_id " );
        sql.append( "from icm_statement_producer )" );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            ResultSet result = statement.executeQuery();
            long added = 0;
            while( result.next() ) {
                added += insertStatementProducer( result.getString( 1 ), 0 );
            }
            return added;
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not update statement producers: " + e, sql.toString(), true );
        }
    }

    /**
     * Update statement producer table with missing records.
     * @param month    int
     * @param year     int
     * @param filetype String
     * @param filesize int
     * @param large    int
     */
    public long updateStatementProducers( int month, int year, String filetype, float filesize, int large ) throws DBOperationsException {
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "select party_id " );
        sql.append( "from icm_statement " );
        sql.append( "where month = ? and " );
        sql.append( "year = ? and " );
        sql.append( "file_type = ? and " );
        sql.append( "bytes    >= ? and " );
        sql.append( "party_id not in ( select party_id " );
        sql.append( "from icm_statement_producer " );
        sql.append( "where ? in (1,-1) )" );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            statement.setInt( 1, month );
            statement.setInt( 2, year );
            statement.setString( 3, filetype );
            statement.setFloat( 4, filesize );
            statement.setInt( 5, large );
            try( ResultSet result = statement.executeQuery() ) {
                long updated = 0;
                while( result.next() ) {
                    updated += updateStatementProducer( result.getString( 1 ), large );
                }
                return updated;
            }
            catch( Exception e ) {
                LOGGER.error( "SQL: " + sql );
                throw new DBOperationsException( "Could not update statement producers: " + e, sql.toString(), true );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not update statement producers: " + e, sql.toString(), true );
        }
    }

    /**
     * Insert into statement producer table.
     * @param producer IcmProducer
     */
    protected long insertStatementProducer( IcmProducer producer ) throws DBOperationsException {
        Timestamp date = new Timestamp( System.currentTimeMillis() );
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "insert into icm_statement_producer " );
        sql.append( " ( party_id, " );
        sql.append( "   large, " );
        sql.append( "   retrieval, " );
        sql.append( "   push_eqc, " );
        sql.append( "   date_added, date_updated ) " );
        sql.append( " values ( ?, ?, ?, ?, ?, ?)" );
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            statement.setString( 1, producer.getPartyId() );
            statement.setInt( 2, producer.isLarge() ? 1 : 0 );
            int retrieval = producer.getRetrieval();
            retrieval = ( retrieval == IcmProducer.PROCESS_REPORT || retrieval == IcmProducer.EXTRACT ? retrieval : IcmProducer.PROCESS_REPORT );
            statement.setInt( 3, retrieval );
            statement.setInt( 4, producer.isPushEcg() ? 1 : 0 );
            statement.setTimestamp( 5, date );
            statement.setTimestamp( 6, date );
            return statement.executeUpdate();
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not insert statement producer: " + e, sql.toString(), true );
        }
    }

    /**
     * Update statement producer table.
     * @param producer IcmProducer
     */
    public long updateStatementProducer( IcmProducer producer ) throws DBOperationsException {
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "update icm_statement_producer " );
        sql.append( "set    large        = ?, " );
        sql.append( "       retrieval    = ?, " );
        sql.append( "       push_eqc     = ?, " );
        sql.append( "       date_updated = ? " );
        sql.append( "where  party_id     = ?" );
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            statement.setInt( 1, producer.isLarge() ? 1 : 0 );
            int retrieval = producer.getRetrieval();
            retrieval = ( retrieval == IcmProducer.PROCESS_REPORT || retrieval == IcmProducer.EXTRACT ? retrieval : IcmProducer.PROCESS_REPORT );
            statement.setInt( 2, retrieval );
            statement.setInt( 3, producer.isPushEcg() ? 1 : 0 );
            statement.setTimestamp( 4, new Timestamp( System.currentTimeMillis() ) );
            statement.setString( 5, producer.getPartyId() );
            long i = statement.executeUpdate();
            if( i == 0 ) {
                i = insertStatementProducer( producer );
            }
            return i;
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not update statement producer: " + e, sql.toString(), true );
        }
    }

    /**
     * Update statement producer table.
     * @param partyId String
     * @param large   int
     * @return long
     */
    public long updateStatementProducer( String partyId, int large ) throws DBOperationsException {
        IcmProducer producer = new IcmProducer( partyId );
        producer.setLarge( large > 0 );
        producer.setRetrieval( 1 );
        producer.setPushEcg( false );
        return updateStatementProducer( producer );
    }

    /**
     * Insert into statement producer table.
     * @param partyId String
     * @param large   int
     */
    protected long insertStatementProducer( String partyId, int large ) throws DBOperationsException {
        IcmProducer p = new IcmProducer( partyId );
        p.setLarge( large == 1 );
        return insertStatementProducer( p );
    }

    /**
     * Get statement details.
     * @param partyId String
     * @param type    String
     * @param year    int
     * @param month   int
     * @return IcmStatement
     */
    public IcmStatement getStatement( String partyId, String type, int year, int month ) {
        IcmStatement stmt = null;
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "SELECT   party_id, file_type, year, month, bytes, rowct, stime " );
        sql.append( "FROM     icm_statement " );
        sql.append( "WHERE    party_id = ? and" );
        sql.append( "         file_type = ? and" );
        sql.append( "         year = ? and" );
        sql.append( "         month = ? " );
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            statement.setString( 1, partyId );
            statement.setString( 2, type );
            statement.setInt( 3, year );
            statement.setInt( 4, month );
            ResultSet result = statement.executeQuery();
            if( result.next() ) {
                stmt = new IcmStatement();
                stmt.setPartyId( result.getString( "party_id" ) );
                stmt.setFileType( result.getString( "file_type" ) );
                stmt.setYear( result.getInt( "year" ) );
                stmt.setMonth( result.getInt( "month" ) );
                stmt.setBytes( result.getLong( "bytes" ) );
                stmt.setRows( result.getLong( "rowct" ) );
                stmt.setTime( result.getString( "stime" ) );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get statement: " + e, sql.toString(), true );
        }
        return stmt;
    }

    /**
     * Update statement.
     * @param partyId String
     * @param type    String
     * @param year    int
     * @param month   int
     * @param bytes   long
     * @param rows    long
     * @param time    String
     */
    public void updateStatement( String partyId, String type, int year, int month, long bytes, long rows, String time ) throws DBOperationsException {
        Timestamp date = new Timestamp( System.currentTimeMillis() );
        type = RunIcmStatements.CSVFTP.equals( type ) ? RunIcmStatements.CSV : type;
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "update icm_statement " );
        sql.append( "set    bytes        = ?, " );
        sql.append( "       rowct        = ?, " );
        sql.append( "       stime        = ?, " );
        sql.append( "       date_updated = ? " );
        sql.append( "where  party_id     = ? and " );
        sql.append( "       file_type    = ? and " );
        sql.append( "       year         = ? and " );
        sql.append( "       month        = ? " );
        LOGGER.debug( "SQL: " + sql );

        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            statement.setLong( 1, bytes );
            statement.setLong( 2, rows );
            statement.setString( 3, time );
            statement.setTimestamp( 4, date );
            statement.setString( 5, partyId );
            statement.setString( 6, type );
            statement.setInt( 7, year );
            statement.setInt( 8, month );
            int i = statement.executeUpdate();
            if( i == 0 ) {
                insertStatement( partyId, type, year, month, bytes, rows, time );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not update statement: " + e, sql.toString(), true );
        }
    }

    /**
     * Insert statement.
     * @param partyId String
     * @param type    String
     * @param year    int
     * @param month   int
     * @param bytes   long
     * @param rows    long
     * @param time    String
     */
    protected void insertStatement( String partyId, String type, int year, int month, long bytes, long rows, String time ) throws DBOperationsException {
        Timestamp date = new Timestamp( System.currentTimeMillis() );
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "insert into icm_statement " );
        sql.append( " ( party_id, file_type, year, month, " );
        sql.append( "   bytes, rowct, stime, " );
        sql.append( "   date_added, date_updated ) " );
        sql.append( " values ( ?, ?, ?, ?, " );
        sql.append( "          ?, ?, ?, " );
        sql.append( "          ?, ?)" );
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            statement.setString( 1, partyId );
            statement.setString( 2, type );
            statement.setInt( 3, year );
            statement.setInt( 4, month );
            statement.setLong( 5, bytes );
            statement.setLong( 6, rows );
            statement.setString( 7, time );
            statement.setTimestamp( 8, date );
            statement.setTimestamp( 9, date );
            statement.executeUpdate();
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not insert statement: " + e, sql.toString(), true );
        }
    }

    /**
     * Get statement runs after start date.
     * @param start LocalDateTime
     * @return List
     */
    public List<IcmStatementRun> getStatementRun( LocalDateTime start ) {
        List<IcmStatementRun> runs = new ArrayList<>();
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "SELECT   * " );
        sql.append( "FROM     icm_statement_run " );
        sql.append( "WHERE    start_date > ? or end_date > ? " );
        sql.append( "ORDER BY start_date" );
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            statement.setTimestamp( 1, ( start != null ? Timestamp.valueOf( start ) : null ) );
            statement.setTimestamp( 2, ( start != null ? Timestamp.valueOf( start ) : null ) );
            ResultSet result = statement.executeQuery();
            while( result.next() ) {
                IcmStatementRun r = new IcmStatementRun();
                r.setType( result.getString( "statement_type" ) );
                r.setCount( result.getInt( "statement_count" ) );
                Timestamp sd = result.getTimestamp( "start_date" );
                r.setStart( sd != null ? DateUtil.parse( sd.getTime() ) : null );
                Timestamp ed = result.getTimestamp( "end_date" );
                r.setEnd( ed != null ? DateUtil.parse( ed.getTime() ) : null );
                runs.add( r );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not get statement run: " + e, sql.toString(), true );
        }
        return runs;
    }

    /**
     * Update statement run.
     * @param statementRunId int
     * @param type           String
     * @param count          int
     * @param start          LocalDateTime
     * @param end            LocalDateTime
     */
    protected void updateStatementRun( int statementRunId, String type, int count, LocalDateTime start, LocalDateTime end ) throws DBOperationsException {
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "update icm_statement_run " );
        sql.append( "set    statement_type   = ?, " );
        sql.append( "       statement_count  = ?, " );
        sql.append( "       start_date       = ?, " );
        sql.append( "       end_date         = ? " );
        sql.append( "where  statement_run_id = ?" );
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString() ) ) {
            statement.setString( 1, type );
            statement.setInt( 2, count );
            statement.setTimestamp( 3, ( start != null ? Timestamp.valueOf( start ) : null ) );
            statement.setTimestamp( 4, ( end != null ? Timestamp.valueOf( end ) : null ) );
            statement.setInt( 5, statementRunId );
            int i = statement.executeUpdate();
            if( i == 0 ) {
                i = insertStatementRun( type, count, start, end );
                LOGGER.info( "No records to update so inserted: " + i );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not update statement run: " + e, sql.toString(), true );
        }
    }

    /**
     * Insert statement run.
     * @param type  String
     * @param count int
     * @param start LocalDateTime
     * @param end   LocalDateTime
     */
    protected int insertStatementRun( String type, int count, LocalDateTime start, LocalDateTime end ) throws DBOperationsException {
        ResultSet keys;
        int statementRunId = 0;
        StringBuilder sql = new StringBuilder( 400 );
        sql.append( "insert into icm_statement_run " );
        sql.append( " ( statement_type, statement_count, start_date, end_date ) " );
        sql.append( " values ( ?, ?, ?, ? )" );
        LOGGER.debug( "SQL: " + sql );
        try( PreparedStatement statement = getConnection().prepareStatement( sql.toString(), Statement.RETURN_GENERATED_KEYS ) ) {
            statement.setString( 1, type );
            statement.setInt( 2, count );
            statement.setTimestamp( 3, ( start != null ? Timestamp.valueOf( start ) : null ) );
            statement.setTimestamp( 4, ( end != null ? Timestamp.valueOf( end ) : null ) );
            statement.executeUpdate();
            keys = statement.getGeneratedKeys();
            if( keys.next() ) {
                statementRunId = keys.getInt( 1 );
                LOGGER.debug( "New statement run: " + statementRunId );
            }
            else {
                LOGGER.error( "No statementRunId key was generated: " + type );
            }
        }
        catch( Exception e ) {
            LOGGER.error( "SQL: " + sql );
            throw new DBOperationsException( "Could not insert statement run: " + e, sql.toString(), true );
        }
        return statementRunId;
    }

    /**
     * Validate producers.
     */
    public void validateProducers() {
        Map<String, IcmProducer> producers = getStatementProducers();
        for( Map.Entry<String, IcmProducer> e : producers.entrySet() ) {
            IcmProducer producer = e.getValue();
            if( producer.getRetrieval() != IcmProducer.PROCESS_REPORT && producer.getRetrieval() != IcmProducer.EXTRACT ) {
                LOGGER.warn( "Found producer [" + producer.getPartyId() + "] with unexpected retrieval value: " + producer.getRetrieval() );
                producer.setRetrieval( IcmProducer.PROCESS_REPORT );
                updateStatementProducer( producer );
            }
        }
    }
}
