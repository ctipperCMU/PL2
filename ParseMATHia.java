import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import java.text.SimpleDateFormat;

public class ParseMATHia {

    /** Constant for the format of dates. */
    public static final SimpleDateFormat DATE_FMT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static String inputFileName = null;
    private static String outputFileName = null;
    private static File debugFile = null;

    // In the first iteration, we had to parse ctContextId for the student id
    private static Boolean origStudentId = false;

    // Include ProblemStartTime. Don't do this. It's the same as the TransactionTime
    // so it results in zero student hours being computed.
    private static Boolean includePST = false;

    private static Set<String> badIds = new TreeSet<String>();

    // Keep track of skill mappings:
    // key: (section, problem, goal)-tuple, value: skill
    private static Map<String, String> skillMap = new HashMap<String, String>();

    public static void main(String[] args) {

        for (int i = 0; i < args.length; i++) {
            String arg = args[i].trim().toLowerCase();
            
            if (arg.equals("-i") || arg.equals("-input")) {
                if (++i < args.length) {
                    inputFileName = args[i];
                } else {
                    System.err.println("A file name must be specified with this arg.");
                    System.exit(-1);
                }
            } else if (arg.equals("-o") || arg.equals("-output")) {
                if (++i < args.length) {
                    outputFileName = args[i];
                } else {
                    System.err.println("A file name must be specified with this arg.");
                    System.exit(-1);
                }
            } else {
                System.out.println("Unrecognized argument: " + args[i]);
            }
        }

        if (inputFileName == null) {
            System.err.println("The input file name must be specified, using the '-i' arg.");
            System.exit(-1);
        }
        if (outputFileName == null) {
            System.out.println("Using outputFileName of 'output.txt'.");
            outputFileName = "output.txt";
        }

        try {
            debugFile = new File("debug.log");
        } catch (Exception e) {
            System.err.println("Failed to create debugging file: " + e.toString());
            debugFile = null;
        }

        debug("Starting... " + new Date());

        Long numLines = 0L;
        try {
            numLines = parseInputFile();
        } catch (Exception e) {
            System.err.println("Failed to parse input file: " + e.toString());
            System.exit(-1);
        }

        debug("Finished parsing " + numLines + " lines at: " + new Date());

        if (badIds.size() > 0) { debug("Found " + badIds.size() + " bad ID(s):"); }
        for (String s : badIds) {
            debug(s);
        }
    }

    private static final String TAB = "\t";
    private static final String NEW_LINE = "\r\n";

    // Headers for incoming data
    private static final String ASSIGNMENT_ID = "assignmentid";
    private static final String CT_CONTEXT_ID = "ctcontextid";
    private static final String SEMANTIC_EVENT_ID = "semanticeventid";
    private static final String SKILL_ID = "skillid";
    private static final String RULE_ID = "ruleid";
    private static final String GOAL_NODE_ID = "goalnodeid";
    private static final String SERVER_TIME = "servertime";
    private static final String ATTEMPT = "attempt";
    private static final String ACTION = "action";
    private static final String TUTOR_OUTCOME = "tutoroutcome";
    private static final String HELP_LEVEL = "helplevel";
    private static final String INPUT = "input";
    private static final String SKILL_PREV_P_KNOWN = "skillpreviouspknown";
    private static final String SKILL_NEW_P_KNOWN = "skillnewpknown";
    private static final String SECTION_NAME = "sectionname";
    private static final String SECTION_PROGRESS_STATUS = "sectionprogressstatus";
    private static final String PROBLEM_ID = "problemid";
    private static final String SCHOOL_ID = "schoolid";

    private static final String[] HEADERS = { ASSIGNMENT_ID,
                                              CT_CONTEXT_ID,
                                              SEMANTIC_EVENT_ID,
                                              SKILL_ID,
                                              RULE_ID,
                                              GOAL_NODE_ID,
                                              SERVER_TIME,
                                              ATTEMPT,
                                              ACTION,
                                              TUTOR_OUTCOME,
                                              HELP_LEVEL,
                                              INPUT,
                                              SKILL_PREV_P_KNOWN,
                                              SKILL_NEW_P_KNOWN,
                                              SECTION_NAME,
                                              SECTION_PROGRESS_STATUS,
                                              PROBLEM_ID,
                                              SCHOOL_ID};

    // Number of lines to process -- read & write -- at a time.
    private static final Integer BATCH_SIZE = 100000;
    
    /**
     * Parse the input file and create the output file.
     */
    private static Long parseInputFile()
        throws Exception
    {
        CSVReader reader = null;
        int lineNum = 1;
        String[] line = null;
        try {

            File outputFile = new File(outputFileName);
            writeHeaders(outputFile);

            File inputFile = new File(inputFileName);

            CSVParser parser = new CSVParserBuilder()
                .withSeparator(',')
                .withIgnoreQuotations(false)
                .build();
            reader = new CSVReaderBuilder(new FileReader(inputFile))
                .withCSVParser(parser)
                .build();

            //            reader = new CSVReader(new FileReader(inputFile), ',');
            line = reader.readNext();
            Map<String, Integer> columnIndexMap = parseHeader(line);

            List<DataShopTxnData> batch = new ArrayList<DataShopTxnData>();

            Iterator<String[]> iterator = reader.iterator();
            while (iterator.hasNext()) {
                line = iterator.next();
                lineNum++;
                if (line != null) { batch.add(parseLine(line, columnIndexMap)); }
                if (batch.size() == BATCH_SIZE) {
                    writeToOutputFile(batch, outputFile);
                    batch = new ArrayList<DataShopTxnData>();
                }
            }
            if (batch.size() > 0) { writeToOutputFile(batch, outputFile); }

        } catch (Exception e) {
            System.err.println("Failed to process line (" + lineNum + "): " + line);
            e.printStackTrace();
            throw e;
        } finally {
            try {
                if (reader != null) { reader.close(); }
            } catch (IOException exception) {
                System.err.println("IOException occurred closing BufferedReader" + exception.toString());
            }
        }

        return reader.getLinesRead();
    }

    /**
     * Read the first line (assumed to be headers) and create a map of names to indices.
     * @param String[] headers
     * @return map of column names to indices
     */
    private static Map<String, Integer> parseHeader(String[] headers) {
        Map<String, Integer> result = new HashMap<String, Integer>();

        int index = 0;
        while (index < headers.length) {
            String h = headers[index];
            for (int i = 0; i < HEADERS.length; i++) {
                if (h.equals(HEADERS[i])) {
                    result.put(HEADERS[i], index);
                    break;
                }
            }
            index++;
        }

        return result;
    }

    /**
     * Read a single line from the input file and create the
     * appropriate output line.
     * @param st String[] the input line
     * @param colIndexMap map of column names to indices
     * @return DataShopTxnData object
     */
    private static DataShopTxnData parseLine(String[] st, Map<String, Integer> colIndexMap) {

        DataShopTxnData result = new DataShopTxnData();

        // TBD. Compare st.length to colIndexMap.size()....
        if (st.length < 17) { return result; }

        String ctContextId = st[colIndexMap.get(CT_CONTEXT_ID)];
        String assignmentId = st[colIndexMap.get(ASSIGNMENT_ID)];
        String action = st[colIndexMap.get(ACTION)];
        String outcome = st[colIndexMap.get(TUTOR_OUTCOME)];
        Date timestamp = new Date(new Long(st[colIndexMap.get(SERVER_TIME)]));

        if (origStudentId) {
            result.setAnonStudentId(parseStudentId(ctContextId, assignmentId));
        } else {
            result.setAnonStudentId(ctContextId);
        }
        result.setSessionId("session1");
        result.setTransactionTime(timestamp);
        result.setAssignmentLevel(assignmentId);
        result.setSectionLevel(st[colIndexMap.get(SECTION_NAME)]);
        result.setProblemName(st[colIndexMap.get(PROBLEM_ID)]);
        result.setSelection(parseSelection(action));
        result.setAction(action);
        result.setInput(st[colIndexMap.get(INPUT)]);
        if (includePST) {
            result.setProblemStartTime(timestamp);
        }
        result.setStepName(st[colIndexMap.get(GOAL_NODE_ID)]);
        result.setOutcome(parseOutcome(outcome));
        result.setStudentResponseType(parseStudentResponseType(action));
        result.setMathiaSkill(st[colIndexMap.get(SKILL_ID)]);
        result.setMathiaNewSkill(computeMathiaNewSkill(st, colIndexMap));
        result.setRuleId(st[colIndexMap.get(RULE_ID)]);
        result.setSkillPreviousPKnown(getPKnown(st[colIndexMap.get(SKILL_PREV_P_KNOWN)]));
        result.setSkillNewPKnown(getPKnown(st[colIndexMap.get(SKILL_NEW_P_KNOWN)]));
        result.setSectionProgressStatus(st[colIndexMap.get(SECTION_PROGRESS_STATUS)]);
        if (colIndexMap.get(SCHOOL_ID) != null) {
            result.setSchoolId(st[colIndexMap.get(SCHOOL_ID)]);
        }
        result.setAttemptAtStep(st[colIndexMap.get(ATTEMPT)]);
        result.setHelpLevel(st[colIndexMap.get(HELP_LEVEL)]);

        return result;
    }

    /**
     * Parse ctcontextid and assignmentid values to determine anonymized student id.
     * The ctcontextid is of the xx_STUDENTID-assignmentid, where xx are two letters, e.g., mx.
     *
     * @param ctContextId
     * @param assignmentId
     * @return String anonymous student id
     */
    private static String parseStudentId(String ctContextId, String assignmentId) {

        int stuIndex = ctContextId.indexOf("-") + 1;
        int assignmentIndex = ctContextId.indexOf(assignmentId) - 1;

        // If assignmentId isn't part of ctContextId, hack it?
        if ((stuIndex < 0) || (assignmentIndex < 0)) { 
            // Keep track of "bad" IDs...
            badIds.add("ctcontextid = " + ctContextId + ", assignmentid = " + assignmentId);
            return ctContextId.substring(3, 37);
        }
        return ctContextId.substring(stuIndex, assignmentIndex);
    }

    private static final String OUTCOME_CORRECT = "CORRECT";
    private static final String OUTCOME_INCORRECT = "INCORRECT";
    private static final String OUTCOME_HINT = "HINT";

    /**
     * Parse the tutoroutcome value to determine the Outcome.
     * @param tutoroutcome one of: OK, JIT, ERROR, INITIAL_HINT, HINT_LEVEL_CHANGE
     * @return String outcome
     */
    private static String parseOutcome(String tutoroutcome) {
        
        if (tutoroutcome.equals("OK")) {
            return OUTCOME_CORRECT;
        } else if (tutoroutcome.equals("JIT") || tutoroutcome.equals("ERROR")) {
            return OUTCOME_INCORRECT;
        } else {   // INITIAL_HINT, HINT_LEVEL_CHANGE
            return OUTCOME_HINT;
        }
    }

    private static final String ACTION_ATTEMPT = "Attempt";
    private static final String ACTION_DONE = "Done";
    private static final String ACTION_HINT_REQUEST = "Hint Request";
    private static final String ACTION_HINT_LEVEL_CHANGE = "Hint Level Change";

    /**
     * Parse the action value to determine the Selection.
     * @param action one of: Attempt, Done, Hint Request, or Hint Level Change
     * @return String select
     */
    private static String parseSelection(String action) {

        if (action.equals(ACTION_ATTEMPT)) {
            return action;
        } else if (action.equals(ACTION_DONE)) {
            return "Done Button";
        } else {    // Hint Request, Hint Level Change
            return "Hint Request dummy";
        }
    }

    /**
     * Parse the action value to determine the Student Response Type.
     * @param action one of: Attempt, Done, Hint Request, or Hint Level Change
     * @return String studentResponseType
     */
    private static String parseStudentResponseType(String action) {
        
        if (action.equals(ACTION_ATTEMPT)) {
            return "ATTEMPT";
        } else if (action.equals(ACTION_DONE)) {
            return "";
        } else {    // Hint Request, Hint Level Change
            return "HINT_REQUEST";
        }
    }

    /**
     * Determine if an untagged transaction should be tagged with current skill.
     * @param in String[] the input line
     * @param colIndexMap map of column names to indices
     * @return String the skill, empty string if not tagged
     */
    private static String computeMathiaNewSkill(String[] in, Map<String, Integer> colIndexMap) {

        String result = "";

        String existingSkill = in[colIndexMap.get(SKILL_ID)];

        StringBuffer sb = new StringBuffer();
        sb.append(in[colIndexMap.get(SECTION_NAME)]).append("_");
        sb.append(in[colIndexMap.get(PROBLEM_ID)]).append("_");
        sb.append(in[colIndexMap.get(GOAL_NODE_ID)]);
        String key = sb.toString();

        String hashedSkill = skillMap.get(key);

        if ((existingSkill != null) && (!existingSkill.trim().equals(""))) {
            result = existingSkill;
            skillMap.put(key, existingSkill);
        } else if (hashedSkill != null) {
            result = hashedSkill;
        }
        
        return result;
    }

    /**
     * Convert String p-known values to Double, allowing for
     * empty, null or "NA" cases.
     */
    private static Double getPKnown(String in) {
        if (in == null) { return null; }
        if (in.trim().equalsIgnoreCase("NA")) { return null; }
        if (in.trim().equalsIgnoreCase("null")) { return null; }
        if (in.trim().equals("")) { return null; }
        return new Double(in);
    }

    private static final String CORRECT = "CORRECT";
    private static final String INCORRECT = "INCORRECT";
    private static final String HINT = "HINT";
    private static final String UNKNOWN = "UNKNOWN";

    /**
     * Convert the event_type in the original data to a DataShop OUTCOME.
     * @param eventType the event_type: C, S, W, F, E
     * @return String the DataShop OUTCOME
     */
    private static String convertToOutcome(String eventType) {
        String result = UNKNOWN;

        if (eventType.equals("C") || eventType.equals("S")) {
            result = CORRECT;
        } else if (eventType.equals("W") || eventType.equals("F")) {
            result = INCORRECT;
        } else if (eventType.equals("E")) {
            result = HINT;
        }

        return result;
    }

    /**
     * Write the headers to the output file.
     * @param outputFile the output file
     */
    public static void writeHeaders(File outputFile) {

        // Setup the writer.
        BufferedWriter bw = null;
        try {
            // Append to existing file.
            FileWriter fw = new FileWriter(outputFile, true);
            bw = new BufferedWriter(fw);

            String headers = getHeaders();
            bw.write(headers);
            bw.write(NEW_LINE);

            bw.close();
        } catch (IOException e) {
            System.err.println("IOException while writing output file.");
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) { }
            }
        }
    }

    /**
     * Write the list of lines to the output file.
     * @param outputLines List of String representing output
     * @param outputFile the output file
     */
    public static void writeToOutputFile(List<DataShopTxnData> outputLines, File outputFile) {

        // Setup the writer.
        BufferedWriter bw = null;
        try {
            // Append to existing file.
            FileWriter fw = new FileWriter(outputFile, true);
            bw = new BufferedWriter(fw);

            int count = 1;
            for (DataShopTxnData o : outputLines) {
                bw.write(formatOutput(o));
                bw.write(NEW_LINE);
                count++;
            }

            bw.close();
        } catch (IOException e) {
            System.err.println("IOException while writing output file.");
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) { }
            }
        }

        debug("Wrote " + outputLines.size() + " line(s) to the output file.");
    }

    private static String getHeaders() {
        StringBuffer sb = new StringBuffer();

        sb.append("Anon Student Id").append(TAB);
        sb.append("Session Id").append(TAB);
        sb.append("Time").append(TAB);
        sb.append("Level (Assignment)").append(TAB);
        sb.append("Level (Section)").append(TAB);
        sb.append("Problem Name").append(TAB);
        sb.append("Step Name").append(TAB);
        sb.append("Selection").append(TAB);
        sb.append("Action").append(TAB);
        sb.append("Input").append(TAB);
        if (includePST) {
            sb.append("Problem Start Time").append(TAB);
        }
        sb.append("Outcome").append(TAB);
        sb.append("KC Model(MATHia)").append(TAB);
        sb.append("KC Model(MATHia New)").append(TAB);
        sb.append("CF (ruleid)").append(TAB);
        sb.append("CF (Skill Previous p-Known)").append(TAB);
        sb.append("CF (Skill New p-Known)").append(TAB);
        sb.append("CF (Section Progress Status)").append(TAB);
        sb.append("CF (schoolid)").append(TAB);
        sb.append("Attempt At Step").append(TAB);
        sb.append("Help Level");

        return sb.toString();
    }

    private static String formatOutput(DataShopTxnData output) {
        StringBuffer sb = new StringBuffer();

        sb.append(output.getAnonStudentId()).append(TAB);
        sb.append(output.getSessionId()).append(TAB);
        sb.append(DATE_FMT.format(output.getTransactionTime())).append(TAB);
        sb.append(output.getAssignmentLevel()).append(TAB);
        sb.append(output.getSectionLevel()).append(TAB);
        sb.append(output.getProblemName()).append(TAB);
        sb.append(output.getStepName()).append(TAB);
        sb.append(output.getSelection()).append(TAB);
        sb.append(output.getAction()).append(TAB);
        sb.append(output.getInput()).append(TAB);
        if (includePST) {
            sb.append(DATE_FMT.format(output.getProblemStartTime())).append(TAB);
        }
        sb.append(output.getOutcome()).append(TAB);
        sb.append(output.getMathiaSkill()).append(TAB);
        sb.append(output.getMathiaNewSkill()).append(TAB);        
        sb.append(output.getRuleId()).append(TAB);
        sb.append(output.getSkillPreviousPKnown()).append(TAB);
        sb.append(output.getSkillNewPKnown()).append(TAB);
        sb.append(output.getSectionProgressStatus()).append(TAB);
        sb.append(output.getSchoolId()).append(TAB);
        sb.append(output.getAttemptAtStep()).append(TAB);
        sb.append(output.getHelpLevel());

        return sb.toString();
    }

    /**
     * Helper method to write message to debugging file.
     * @param msg Message to write
     */
    private static void debug(String msg) {

        // Setup the writer.
        BufferedWriter bw = null;
        try {
            // Append to existing file.
            FileWriter fw = new FileWriter(debugFile, true);
            bw = new BufferedWriter(fw);

            bw.write(msg);
            bw.write(NEW_LINE);

            bw.close();
        } catch (IOException e) {
            System.err.println("IOException while writing output file.");
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) { }
            }
        }
    }

    private static class DataShopTxnData {
        public DataShopTxnData() {} 

        private String anonStudentId;
        private String sessionId;
        private Date transactionTime;
        private String assignmentLevel;
        private String sectionLevel;
        private String problemName;
        private String selection;
        private String action;
        private String input;
        private Date problemStartTime;
        private String stepName;
        private String outcome;
        private String studentResponseType;
        private String tutorResponseType;
        private String mathiaSkill;
        private String mathiaNewSkill;
        private String ruleId;
        private Double skillPreviousPKnown;
        private Double skillNewPKnown;
        private String sectionProgressStatus;
        private String schoolId;
        private String attemptAtStep;
        private String helpLevel;

        public String getAnonStudentId() { return anonStudentId; }
        public void setAnonStudentId(String anonStudentId) { this.anonStudentId = anonStudentId; }
        public String getSessionId() { return sessionId; }
        public void setSessionId(String sessionId) { this.sessionId = sessionId; }
        public Date getTransactionTime() { return transactionTime; }
        public void setTransactionTime(Date transactionTime) { this.transactionTime = transactionTime; }
        public String getAssignmentLevel() { return assignmentLevel; }
        public void setAssignmentLevel(String assignmentLevel) { this.assignmentLevel = assignmentLevel; }
        public String getSectionLevel() { return sectionLevel; }
        public void setSectionLevel(String sectionLevel) { this.sectionLevel = sectionLevel; }
        public String getProblemName() { return problemName; }
        public void setProblemName(String problemName) { this.problemName = problemName; }
        public String getSelection() { return selection; }
        public void setSelection(String selection) { this.selection = selection; }
        public String getAction() { return action; }
        public void setAction(String action) { this.action = action; }
        public String getInput() { return input; }
        public void setInput(String input) { this.input = input; }
        public Date getProblemStartTime() { return problemStartTime; }
        public void setProblemStartTime(Date problemStartTime) { this.problemStartTime = problemStartTime; }
        public String getStepName() { return stepName; }
        public void setStepName(String stepName) { this.stepName = stepName; }
        public String getOutcome() { return outcome; }
        public void setOutcome(String outcome) { this.outcome = outcome; }
        public String getStudentResponseType() { return studentResponseType; }
        public void setStudentResponseType(String studentResponseType) { this.studentResponseType = studentResponseType; }
        public String getTutorResponseType() { return tutorResponseType; }
        public void setTutorResponseType(String tutorResponseType) { this.tutorResponseType = tutorResponseType; }
        public String getMathiaSkill() { return mathiaSkill; }
        public void setMathiaSkill(String mathiaSkill) { this.mathiaSkill = mathiaSkill; }
        public String getMathiaNewSkill() { return mathiaNewSkill; }
        public void setMathiaNewSkill(String mathiaNewSkill) { this.mathiaNewSkill = mathiaNewSkill; }
        public String getRuleId() { return ruleId; }
        public void setRuleId(String ruleId) { this.ruleId = ruleId; }
        public Double getSkillPreviousPKnown() { return skillPreviousPKnown; }
        public void setSkillPreviousPKnown(Double skillPreviousPKnown) { this.skillPreviousPKnown = skillPreviousPKnown; }
        public Double getSkillNewPKnown() { return skillNewPKnown; }
        public void setSkillNewPKnown(Double skillNewPKnown) { this.skillNewPKnown = skillNewPKnown; }
        public String getSectionProgressStatus() { return sectionProgressStatus; }
        public void setSectionProgressStatus(String sectionProgressStatus) { this.sectionProgressStatus = sectionProgressStatus; }
        public String getSchoolId() { return schoolId; }
        public void setSchoolId(String schoolId) { this.schoolId = schoolId; }
        public String getAttemptAtStep() { return attemptAtStep; }
        public void setAttemptAtStep(String attemptAtStep) { this.attemptAtStep = attemptAtStep; }
        public String getHelpLevel() { return helpLevel; }
        public void setHelpLevel(String helpLevel) { this.helpLevel = helpLevel; }

        public String toString() {
            StringBuffer sb = new StringBuffer("DataShopTxnData [");
            sb.append("Anon Student Id = ").append(getAnonStudentId());
            sb.append(", Session Id = ").append(getSessionId());
            sb.append(", MATHia skillId = ").append(getMathiaSkill());
            sb.append(", MATHia New skillId = ").append(getMathiaNewSkill());
            sb.append(", Transaction Time = ").append(getTransactionTime());
            sb.append(", Level (Assignment) = ").append(getAssignmentLevel());
            sb.append(", Level (Section) = ").append(getSectionLevel());
            sb.append(", Problem Name = ").append(getProblemName());
            sb.append(", CF (Skill Previous p-Known) = ").append(getSkillPreviousPKnown());
            sb.append(", CF (Skill New p-Known) = ").append(getSkillNewPKnown());
            sb.append("]");

            return sb.toString();
        }
    }
}