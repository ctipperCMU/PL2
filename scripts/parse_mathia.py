
import pandas as pd
import argparse
import datetime as dt
from dateutil.parser import parse

inFile = ""
outFile = ""

parser = argparse.ArgumentParser(description='Parser for MATHia-to-DataShop transform')
parser.add_argument('-inFile', type=str, help='input file', required=True)
parser.add_argument('-outFile', type=str, help='output file', required=True)
args, option_file_index_args = parser.parse_known_args()
inFile = args.inFile
outFile = args.outFile

# input file column headers
ASSIGNMENT_ID = "assignmentid"
CT_CONTEXT_ID = "ctcontextid"
SEMANTIC_EVENT_ID = "semanticeventid"
SKILL_ID = "skillid"
RULE_ID = "ruleid"
GOAL_NODE_ID = "goalnodeid"
SERVER_TIME = "servertime"
ATTEMPT = "attempt"
ACTION_IN = "action"
TUTOR_OUTCOME = "tutoroutcome"
HELP_LEVEL_IN = "helplevel"
INPUT_IN = "input"
SKILL_PREV_P_KNOWN = "skillpreviouspknown"
SKILL_NEW_P_KNOWN = "skillnewpknown"
SECTION_NAME = "sectionname"
SECTION_PROGRESS_STATUS = "sectionprogressstatus"
PROBLEM_ID = "problemid"
SCHOOL_ID = "schoolid"

# input constants
CORRECT = "CORRECT"
INCORRECT = "INCORRECT"
HINT = "HINT"
UNKNOWN = "UNKNOWN"
ACTION_ATTEMPT = "Attempt"
ACTION_DONE = "Done"
ACTION_HINT_REQUEST = "Hint Request"
ACTION_HINT_LEVEL_CHANGE = "Hint Level Change"

# input file columns that we'll eventually drop
dropColumns = [ASSIGNMENT_ID,
               CT_CONTEXT_ID,
               SEMANTIC_EVENT_ID,
               SKILL_ID,
               RULE_ID,
               GOAL_NODE_ID,
               SERVER_TIME,
               ATTEMPT,
               ACTION_IN,
               TUTOR_OUTCOME,
               HELP_LEVEL_IN,
               INPUT_IN,
               SKILL_PREV_P_KNOWN,
               SKILL_NEW_P_KNOWN,
               SECTION_NAME,
               SECTION_PROGRESS_STATUS,
               PROBLEM_ID,
               SCHOOL_ID]

# output file column headers
ANON_STUDENT_ID = "Anon Student Id"
SESSION_ID = "Session Id"
TIME = "Time"
LEVEL_ASSIGNMENT = "Level (Assignment)"
LEVEL_SECTION = "Level (Section)"
PROBLEM_NAME = "Problem Name"
STEP_NAME = "Step Name"
SELECTION = "Selection"
ACTION_OUT = "Action"
INPUT_OUT = "Input"
OUTCOME = "Outcome"
KCM_MATHIA = "KC Model(MATHia)"
KCM_MATHIA_NEW = "KC Model(MATHia New)"
CF_RULE_ID = "CF (ruleid)"
CF_SKILL_P_KNOWN = "CF (Skill Previous p-Known)"
CF_SKILL_NEW_P_KNOWN = "CF (Skill New p-Known)"
CF_SECTION_PROGRESS_STATUS = "CF (Section Progress Status)"
CF_SCHOOL_ID = "CF (schoolid)"
ATTEMPT_AT_STEP = "Attempt At Step"
HELP_LEVEL_OUT = "Help Level"

# output columns, in the order we want them
orderedColumns = [ANON_STUDENT_ID,
                  SESSION_ID,
                  TIME,
                  LEVEL_ASSIGNMENT,
                  LEVEL_SECTION,
                  PROBLEM_NAME,
                  STEP_NAME,
                  SELECTION,
                  ACTION_OUT,
                  INPUT_OUT,
                  OUTCOME,
                  KCM_MATHIA,
                  KCM_MATHIA_NEW,
                  CF_RULE_ID,
                  CF_SKILL_P_KNOWN,
                  CF_SKILL_NEW_P_KNOWN,
                  CF_SECTION_PROGRESS_STATUS,
                  CF_SCHOOL_ID,
                  ATTEMPT_AT_STEP,
                  HELP_LEVEL_OUT]


def handleSimpleColumns(x):
    x[ANON_STUDENT_ID] = x[CT_CONTEXT_ID]
    x[SESSION_ID] = "session1"
    x[LEVEL_ASSIGNMENT] = x[ASSIGNMENT_ID]
    x[LEVEL_SECTION] = x[SECTION_NAME]
    x[PROBLEM_NAME] = x[PROBLEM_ID]
    x[ACTION_OUT] = x[ACTION_IN]
    x[INPUT_OUT] = x[INPUT_IN]
    x[STEP_NAME] = x[GOAL_NODE_ID]
    x[KCM_MATHIA] = x[SKILL_ID]
    x[CF_RULE_ID] = x[RULE_ID]
    x[CF_SKILL_P_KNOWN] = x[SKILL_PREV_P_KNOWN]
    x[CF_SKILL_NEW_P_KNOWN] = x[SKILL_NEW_P_KNOWN]
    x[CF_SECTION_PROGRESS_STATUS] = x[SECTION_PROGRESS_STATUS]
    x[CF_SCHOOL_ID] = x[SCHOOL_ID]
    x[ATTEMPT_AT_STEP] = x[ATTEMPT]
    x[HELP_LEVEL_OUT] = x[HELP_LEVEL_IN]
    return x

def handleOtherColumns(x):
    x[TIME] = dt.datetime.fromtimestamp(float(x[SERVER_TIME]) / 1000).strftime('%Y-%m-%d %H:%M:%S')
    x[SELECTION] = parseSelection(x[ACTION_IN])
    x[OUTCOME] = parseOutcome(x[TUTOR_OUTCOME])
    x[KCM_MATHIA_NEW] = computeMathiaNewSkill(x[SKILL_ID], x[SECTION_NAME], x[PROBLEM_ID], x[GOAL_NODE_ID])
    return x

def parseSelection(y):
   result = y if y == 'Attempt' else ('Done Button' if y == 'Done' else 'Hint Request dummy')
   return result

def parseOutcome(y):
    result = CORRECT if y == 'OK' else (INCORRECT if y == 'JIT' or y == 'ERROR' else HINT)
    return result

kcmDict = {}

def computeMathiaNewSkill(skillId, section, problemId, goalNodeId):
    result = ''
    key = section + '_' + problemId

    if goalNodeId and pd.notna(goalNodeId):
        key = key + '_' + goalNodeId

    hashedSkill = ''
    if key in kcmDict:
        hashedSkill = kcmDict[key]

    if skillId and pd.notna(skillId):
        result = skillId
        kcmDict[key] = skillId
    elif hashedSkill and pd.notna(hashedSkill) and hashedSkill != '':
        result = hashedSkill

    return result

# Read the input file and handle anything that should be "NA".
df = pd.read_csv(inFile, dtype=str, na_values = ['NA', 'null'])

# Some of the columns are a simple copy...
df = df.apply(handleSimpleColumns, axis=1)

# ... others require a bit of processing.
df = df.apply(handleOtherColumns, axis=1)

# Now drop the columns from the original file.
df.drop(dropColumns, axis=1, inplace=True)

# Likely not necessary, but order the output columns for DataShop.
df = df.reindex(columns = orderedColumns)

# Finally! Write the output file, using empty string for NA values.
df.to_csv(outFile, sep='\t', index=False, na_rep = '')


