create or replace procedure cmp_tlc_fu_write_log is

      PROCEDURE pStats( acTable VARCHAR2, anPerc NUMBER DEFAULT 0.01) IS
      BEGIN
          AP_PUBLIC.CORE_LOG_PKG.pStart( 'Stat:'||upper(acTable) );
          DBMS_STATS.Gather_Table_Stats( OwnName => 'AP_CRM', TabName => upper(acTable),Estimate_Percent => anPerc );
          AP_PUBLIC.CORE_LOG_PKG.pEnd;
      END;
      PROCEDURE pTruncate( acTable VARCHAR2) IS
      BEGIN
          AP_PUBLIC.CORE_LOG_PKG.pStart( 'Trunc:'||upper(acTable) );
          EXECUTE IMMEDIATE 'TRUNCATE TABLE AP_CRM.'||upper(acTable) ;
          AP_PUBLIC.CORE_LOG_PKG.pEnd ;
      END;
begin
      AP_PUBLIC.CORE_LOG_PKG.pInit( 'AP_CRM', 'CMP_TLC_FU_WRITE_LOG');
      pstats('log_cmp_tlc_fu_base');
      AP_PUBLIC.CORE_LOG_PKG.pStart('Insert into LOG_CMP_TLC_FU_BASE');
      insert /*+ APPEND */ into log_cmp_tlc_fu_base
      select trunc(sysdate)log_date, to_char(sysdate,'hh24:mi:ss')time_inserted, t.* from cmp_tlc_fu_base t;
      AP_PUBLIC.CORE_LOG_PKG.pEnd;
      commit;
      pstats('log_cmp_tlc_fu_base');

      pstats('log_cmp_tlc_fu_call_list');
      AP_PUBLIC.CORE_LOG_PKG.pStart('Insert into LOG_CMP_TLC_FU_CALL_LIST');
      insert /*+ APPEND */ into log_cmp_tlc_fu_call_list
      select trunc(sysdate)log_date, to_char(sysdate,'hh24:mi:ss')time_inserted, t.* from cmp_tlc_fu_call_list t;
      AP_PUBLIC.CORE_LOG_PKG.pEnd;
      commit;
      pstats('log_cmp_tlc_fu_call_list');

      pstats('log_cmp_tlc_fu_comm_list');
      AP_PUBLIC.CORE_LOG_PKG.pStart('Insert into LOG_CMP_TLC_FU_COMM_LIST');
      insert /*+ APPEND */ into log_cmp_tlc_fu_comm_list
      select trunc(sysdate)log_date, to_char(sysdate,'hh24:mi:ss')time_inserted, t.* from cmp_tlc_fu_comm_list t;
      AP_PUBLIC.CORE_LOG_PKG.pEnd;
      commit;
      pstats('log_cmp_tlc_fu_comm_list');
      AP_PUBLIC.CORE_LOG_PKG.pFinish;
end;
/

