// Portland General Electric
// Maximo Work Actuals Export

// This script performs an incremental export of data from EmpCenter
// to Maximo.  Only time associated with Maximo work orders will be
// included in this export.  (Maximo work orders are identified by an
// FWO value beginning with 'M'.)  No amendments to prior pay periods
// will be included.

// Version history
//  v1.0    09/24/2012  GW
//  v1.1    03/12/2013  GW  filtering out records that sum to zero hours (#728)
//  v1.2    10/02/2013 Aaron McConnell - SOW 79452 - zero out rate if employee's union code is null
//  v1.3    01/22/2014  MH  Adding in transit LD field (UNIT9) to export (SOW 0304.009).
//  v2.0	11/16/2018  ailyas Upgraded to 18.3 <INT-7841>

includeDistributedPolicy("INCREMENTAL_EXPORT_API");
includeDistributedPolicy("FILE_WRITER_API");
includePolicy("FILE_PATH_LIBRARY");
includeDistributedPolicy("EMPLOYEE_IMPORT_LIBRARY");
includeDistributedPolicy("API_UTIL");
includeDistributedPolicy("JS_UTIL");
includeDistributedPolicy("POLICY_SET_API");
includeDistributedPolicy("PERSON_DATA_API");
includeDistributedPolicy("GENERIC_EXPORT_LIBRARY_BRANCH_A_3");
//********** CONSTANTS ************************************

var FILE_PATH = EXPORT_PATH;
var FILE_NAME = "WFS_MAX_ActualHours.csv";

var TARGET_NAME = "MAXIMO_EXPORT";                                                        // Incremental export target name
var NEGATE_FIELDS = true;
var RETRACT_ON_TERM = true;


var PAY_CODE_SET = "PGE_MAXIMO_EXPORT_PAY_CODES";                                         // Pay codes to export
var DOUBLE_RATE_PAY_CODES = ["GOLDEN_TIME", "HIGH_TIME", "PREMIUM_UPGRADE_PAY"];          // Select pay codes trigger double rate
var FIELD_NAMES = "HOURS,FWO,EMPLID,WORK_DATE,PAY_CODE,RATE,JOB_CODE,INTRANSIT";          // Fields in export file
var GROUP_FIELDS = ["DISPLAY_EMPLOYEE", "WORK_DT", "LD1", "PAY_CODE", "EFF_RATE", "LD8"]; // Fields to group by for summarization

// added 08/14/2014 to allow more than just codes starting with 'M' to be accepted, and more easily allow new additions.
var VALID_FWO_PREFIXES = ["M","F"];                                                       // We only want to export FWO's that begin with a character listed in this array.

var WRITE_HEADER = false;
var DELIMITER = ",";

var DATE_FMT = "MM/dd/yyyy";
var NUM_FMT = "#0.00";
var DEBUG = false;

var INCR_EXPORT_TARGET_PARMS = {
  // exportTarget {String} - unique name for the export process. Used to
  //  differentiate between multiple incremental exports a customer may have so
  //  that changes calculated in one export do not impact the changes
  //  calculated by a different export.
  exportTarget: TARGET_NAME,
  // enableDebugLogging {Boolean} - true if debug logging should be generated.
  enableDebugLogging: DEBUG
};


var INCR_EXPORT_DIFF_PARMS = {
  // startDate {WDate} - starting date of range to be be evaluated by the export.
  startDate: findCurrentPeriod().getStart(),
  // endDate {WDate} - ending date of range to be evaluated by the export.
  endDate: findCurrentPeriod().getEnd(),
  // negateNumericFields {Boolean} - should numerical output fields be negated
  //  on reversal.
  negateNumericFields: NEGATE_FIELDS,
  // retractOnTerm {Boolean} - should previously-exported time be retracted
  //  following a termination.
  retractOnTerm: RETRACT_ON_TERM,
  // priorPeriodMode {String} - defines when prior-period time should be
  //  included in the export.
  //  PRIOR_PERIOD_ON_APPROVAL: Prior period on approval
  //  PRIOR_PERIOD_ON_CHANGE: Prior period on change
  //  PRIOR_PERIOD_UPON_LOCK: Prior period upon lock
  periodPeriodMode: "PRIOR_PERIOD_ON_CHANGE",
  // gracePeriod {Integer} - time in minutes before the previous export run time
  //  to use when checking for timesheet modifications.
  gracePeriod: 1440,
  // logData {Boolean} - indicates if information about the differences being
  //  computed should be written to the job log.
  logData: true
};


//********** OBJECTS **************************************

function WorkActualsData() {
  this.data = new LookupTable();
}

function WorkActualsData_getMatchKey(record) {
  var key = "|";
  for (var i in GROUP_FIELDS)
    key += record[GROUP_FIELDS[i] ] + "|";
  return key;
}

function WorkActualsData_addData(record) {
  log.info("\tProcessing record for work order = " + record.LD1 + ", pay code = " + record.PAY_CODE + ", hours = " + record.HOURS + ", Transit Time = " + record.UNIT9);

  // Create an entry for this key if it doesn't already exist
  var matchKey = this.getMatchKey(record);
  if (!this.data.containsKey(matchKey) ) {
    var initData = new Object();
    initData.fwo        = record.LD1;                   // Field work order
    initData.emplid     = record.DISPLAY_EMPLOYEE;      // Employee number
    initData.workDt     = convertWDate(record.WORK_DT); // Work date
    initData.payCode    = record.PAY_CODE;              // Pay code
    initData.rate       = inArray(DOUBLE_RATE_PAY_CODES, record.PAY_CODE) ? 2 * record.EFF_RATE : record.EFF_RATE;  // Rate (doubled for select pay codes)
    initData.jobCode    = record.LD8;                   // Job code
    initData.inTransit  = 0;                            //In Transit summarized
    initData.hours      = 0;                            //hours exported will be the time sheet hours field minus the in transit time, stored in UNIT9.
    this.data.set(matchKey, initData);
  }

  // Increment the hours
  this.data.get(matchKey).hours += (record.HOURS - record.UNIT9);
  
  this.data.get(matchKey).inTransit += record.UNIT9;
  

  // For the double-rate pay codes, need to subtract the hours from the WORKED_ALLOCATED_REG time
  if (inArray(DOUBLE_RATE_PAY_CODES, record.PAY_CODE) ) {
    var adjRecord = new Object();
    for (var i in GROUP_FIELDS)
      adjRecord[GROUP_FIELDS[i] ] = record[GROUP_FIELDS[i] ];
    adjRecord.EFF_RATE = record.EFF_RATE;
    adjRecord.PAY_CODE = "WORKED_ALLOCATED_REG";
    adjRecord.HOURS = -1 * (record.HOURS - record.UNIT9);
    adjRecord.UNIT9 = -1 * record.UNIT9;  //Unit9 (transit time) should be 0(zero) for double time and overtime records.
    this.addData(adjRecord);
  }
}

function WorkActualsData_writeData(writer) {
var keylist = this.data.getKeyArray();
for(var i=0;i<keylist.length ;i++){ 
   if (this.data.get(keylist[i]).hours == 0) 
	    continue;
var genExportAPI = new GenericExportSpecAPI();
    writer.append(genExportAPI.formatNumber((this.data.get(keylist[i]).hours),NUM_FMT), false);     // Hours
    writer.append(this.data.get(keylist[i]).fwo, false);                       // Field work order
    writer.append(this.data.get(keylist[i]).emplid, false);                    // Employee number
    writer.append(this.data.get(keylist[i]).workDt.toString(DATE_FMT), false); // Work date
    writer.append(this.data.get(keylist[i]).payCode, false);                   // Pay code
    writer.append(genExportAPI.formatNumber((this.data.get(keylist[i]).rate),NUM_FMT), false);      // Rate
    writer.append(this.data.get(keylist[i]).jobCode, false);                   // Job code
    writer.append(this.data.get(keylist[i]).inTransit,false);                  // In Transit
    writer.writeBuffer();
  }
}

WorkActualsData.prototype.addData = WorkActualsData_addData;
WorkActualsData.prototype.getMatchKey = WorkActualsData_getMatchKey;
WorkActualsData.prototype.writeData = WorkActualsData_writeData;


//********** FUNCTIONS ************************************

// Writes the header row for a file using the specified field names and writer
function writeHeaderRow(writer, fieldNames) {
  writer.append(fieldNames, false);
  writer.writeBuffer();
}

// Processes each employee and determines which records should be exported
function processExportRecords(incrExportAPI, dateRange, writer) {

var endDate = dateRange.getEnd();

	var match = new MatchCondition(MatchTable.EMPLOYEE, "END_EFF_DT", MatchOperator.GREATER_THAN_OR_EQUALS, endDate);
	match.and(new MatchCondition(MatchTable.EMPLOYEE, "EFF_DT", MatchOperator.LESS_THAN_OR_EQUALS, endDate));
	match.and(new MatchCondition(MatchTable.ASGNMT, "END_EFF_DT", MatchOperator.GREATER_THAN_OR_EQUALS, endDate));
	match.and(new MatchCondition(MatchTable.ASGNMT, "EFF_DT", MatchOperator.LESS_THAN_OR_EQUALS, endDate));
	match.and(new MatchCondition(MatchTable.ASGNMT_MASTER, "ASGNMT_TYPE", MatchOperator.EQUALS, 1));

/*   var empStr = "select e.*, am.asgnmt, a.policy_profile " +
    "from asgnmt_master am, employee e, asgnmt a where " +
    "am.employee = e.employee " +
    //"and (e.display_employee = '" + employeeId + "') " +
    "and am.asgnmt_type = 1 " +
    "and am.asgnmt = a.asgnmt " +
    "and ? between e.eff_dt and e.end_eff_dt " +
    "and ? between a.eff_dt and a.end_eff_dt " +
    "order by e.display_employee, am.asgnmt";
  var empRecord = new Sql(connection, empStr);
  empRecord[1] = dateRange.getEnd();
  empRecord[2] = dateRange.getEnd(); */

    // Get changed records
	var exportBatchId = incrExportAPI.computeDifferences(match, INCR_EXPORT_DIFF_PARMS);

    var curData = new Object();
    for (var g in GROUP_FIELDS)
      curData[GROUP_FIELDS[g] ] = null;
    var summedRecords = new WorkActualsData();

    // Sort the records by vehicle and period end date
	var exportRecords = filterAndSortBatchRecords(incrExportAPI, exportBatchId);
	if (exportRecords == null) {
		log.info("No Record(s) Found");
    }
    for (var i in exportRecords) {
      jsHost.throwIfAbortRequested();

      try {
        var Record = exportRecords[i];

        // Determine if there has been a change to any of the fields being used for grouping, and write the record if there has been a change
        var groupingChange = false;
        for (var g in GROUP_FIELDS) {
          if (Record[GROUP_FIELDS[g] ] != curData[GROUP_FIELDS[g] ] ) {
            groupingChange = true;
            break;
          }
        }
        if (groupingChange) {
          summedRecords.writeData(writer);
          summedRecords = new WorkActualsData();
          for (var g in GROUP_FIELDS)
            curData[GROUP_FIELDS[g] ] = Record[GROUP_FIELDS[g] ];
        }

        // Only export records with specific pay codes
        if (!inArray(payCodeSet,Record.PAY_CODE)) 
          continue;
        // Only export records using Maximo work orders //08/13/2014 added additional work orders types to exported records.
        var prefix = Record.ld1.substr(0, 1).toUpperCase();
        if (!inArray(VALID_FWO_PREFIXES, prefix) )
          continue;

        // Aaron McConnell 10/1/2013 - null out rate if employee's union code (os6) is null
        if (isUnionCodeNull(Record.display_employee)) {
          log.debug("Union code for employee " + Record.display_employee + " is null, setting rate to 0.");
          Record.eff_rate = 0;
        }

        summedRecords.addData(Record);
//diff end
        log.incrementRecordCount();
      } catch (e) {
        log.error("Error exporting record : " + e);
        if (typeof Record != "undefined") {
          incrExportAPI.rollbackRecord(Record);
        }
      }
    }

    summedRecords.writeData(writer);
  }


/**
 * Build a map of display employee to union code (other_string6 on the employee record)
 *
 * @param {Connection} connection the database connection to the workforce database
 * @param {WDateTime} timestamp the timestamp to use for effective dating
 * @return {Object (map of String->String)} a map of display employee to union code
 */
function buildUnionCodeMap(timestamp) {
  var returnMap = {};
	
	var personDataAPI = new PersonDataAPI();
	var results = personDataAPI.getAllEmployees(null,timestamp);
	
/*   var query = "SELECT display_employee, other_string6 union_code from employee WHERE ? between eff_dt and end_eff_dt";
  var results = new Sql(connection, query, timestamp); */

   for (var i in results) {
    var displayEmployee = results[i].display_employee;
    var unionCode = results[i].other_string6;

    returnMap[displayEmployee] = unionCode;
  }

  return returnMap;
}

/**
 * Determine if the union code for a given display_employee is null/undefined/empty
 *
 * @param {String} display_employee the display employee value to look up
 * @return {Boolean} true if no union code is available for the employee
 */
function isUnionCodeNull(display_employee) {
  if (!unionCodeMap) {
    throw "Union Code mapping is not available";
  }

  var unionCode = unionCodeMap[display_employee]; // Undefined if no record exists for the employee
  return isBlank(unionCode);
}


// Defines an ordering for incr_exp_hist_detail records
// Ordered by work date, pay code, job code, fwo, rate (assuming only records for one employee)
function filterAndSortBatchRecords(incrExApi, batchID) {
  var recParams = {
    orderFields: ["DISPLAY_EMPLOYEE","ASGNMT","WORK_DT", "PAY_CODE", "LD8", "LD1", "EFF_RATE"]
  };
  var records = incrExApi.getExportDetails(batchID, recParams);
  return records;
}

// Determine the current period begin date, to exclude any amendments
function findCurrentPeriod(){
	var policyProfilePeriodStatus = getPolicyProfilePeriodStatus('PGE_EXEMPT');
	if (policyProfilePeriodStatus.PPG == 'PGE') {
	var ppBegin = policyProfilePeriodStatus.CurPrdBeg;
	  log.info("Current period begin date = " + ppBegin);
	}
 
    var dateRange = new WFSDateRange(ppBegin.addMonths(-3), WFSDate.today().addMonths(1));
	return dateRange;
}

function main() {
  try {
    exportTimeStamp = WFSDateTime.now();

    log.info("Exporting data to " + FILE_PATH + FILE_NAME);
	var writerParms = {
      fileName: FILE_PATH + FILE_NAME,
      delimiter: DELIMITER,
      useQuotes: false
    };
    var writer = new FileWriter(writerParms);
    if (WRITE_HEADER)
      writeHeaderRow(writer, FIELD_NAMES);

    // Load the pay code set to export
	var policySetAPI = new PolicySetAPI();
    var today = WFSDate.today();

    payCodeSet      = policySetAPI.getPoliciesInSet('PAY_CODE', PAY_CODE_SET, today);
    unionCodeMap = buildUnionCodeMap(exportTimeStamp);
 
    // Start the export
    var dateRange = findCurrentPeriod();
    log.info("Export target = " + TARGET_NAME + ", date range from " + dateRange.getStart() + " to " + dateRange.getEnd() );
    var incrExpAPI = new IncrementalExportAPI(INCR_EXPORT_TARGET_PARMS);   
    processExportRecords(incrExpAPI, dateRange, writer);

   }
  catch (e) {
    log.error("Error: " + e);
	incrExpAPI.rollbackExport(exportBatchId);
  }
  finally {
    if (writer)             writer.close();
  }
}


main();