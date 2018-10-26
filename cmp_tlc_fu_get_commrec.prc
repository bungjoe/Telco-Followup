create or replace procedure cmp_tlc_fu_get_commrec is
      SKP_COMM_CHANNEL NUMBER;
      SKP_COMM_TYPE NUMBER;
      SKP_COMM_SUBTYPE NUMBER;
      SKP_COMM_SUBTYP_SPEC NUMBER;
      SKP_COMM_SUBTYP_SUB_SPEC NUMBER;
      SKP_COMM_STATUS NUMBER;
      SKP_COMM_RESULT_TYPE NUMBER;
      sql_string varchar2(10000);

      cursor cfg_commlist is
          SELECT skp_communication_channel, skp_communication_type, skp_communication_subtype, skp_comm_subtype_specif,
                 skp_comm_subtype_sub_specif, skp_communication_status, skp_communication_result_type
          from CAMP_CFG_COMM_LIST csm where name_campaign = 'Telco FU' and active = 'Y';

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
      AP_PUBLIC.CORE_LOG_PKG.pInit( 'AP_CRM', 'CMP_TLC_FU_GET_COMMREC');
      AP_PUBLIC.CORE_LOG_PKG.pStart('Chk:IS_DWH_LOAD_FINISHED');
      ap_ops.p_is_dwh_load_finished('AP_CRM','CHAIN_FF_CL',null);
      AP_PUBLIC.CORE_LOG_PKG.pEnd;

      ptruncate('gtt_cmp_rtn_02_commlist');

      sql_string :=
'begin insert /*+ APPEND */ into ap_crm.gtt_cmp_rtn_02_commlist
select /*+ PARALLEL(5) USE_HASH (fcr dcc ccl cct ccs css csf cst crt) */
       fcr.date_call, fcr.dtime_inserted, fcr.skf_communication_record, fcr.skp_client, fcr.skp_credit_case,
       fcr.skp_communication_channel, fcr.code_channel, ccl.name_communication_channel,
       fcr.skp_communication_type, fcr.code_type_code, cct.NAME_COMMUNICATION_TYPE,
       fcr.skp_communication_subtype, fcr.code_subtype, ccs.NAME_COMMUNICATION_SUBTYPE,
       fcr.skp_comm_subtype_specif, css.CODE_COMM_SUBTYPE_SPECIF, css.NAME_COMM_SUBTYPE_SPECIF,
       fcr.skp_comm_subtype_sub_specif, csf.CODE_COMM_SUBTYPE_SUB_SPECIF, csf.NAME_COMM_SUBTYPE_SUB_SPECIF,
       fcr.skp_communication_status, fcr.code_status, cst.name_communication_status,
       fcr.skp_communication_result_type, fcr.code_result_type, crt.NAME_COMMUNICATION_RESULT_TYPE,
       fcr.text_note, fcr.text_contact, fcr.employee_number, fcr.common_name
FROM OWNER_dWH.f_communication_record_tt fcr
left join owner_Dwh.dc_credit_case dcc on fcr.skp_credit_case = dcc.skp_credit_case
left join owner_dwh.cl_communication_channel ccl on fcr.skp_communication_channel = ccl.skp_communication_channel
left join owner_dwh.cl_communication_type cct on fcr.skp_communication_type = cct.SKP_COMMUNICATION_TYPE
left join owner_dwh.cl_communication_subtype ccs on fcr.skp_communication_subtype = ccs.skp_communication_subtype
left join owner_dwh.cl_comm_subtype_specif css on fcr.skp_comm_subtype_specif = css.SKP_COMM_SUBTYPE_SPECIF
left join owner_dwh.cl_comm_subtype_sub_specif csf on fcr.skp_comm_subtype_sub_specif = csf.SKP_COMM_SUBTYPE_SUB_SPECIF
left join owner_dwh.cl_communication_status cst on fcr.skp_communication_status = cst.skp_communication_status
left join owner_Dwh.cl_communication_result_type crt on fcr.skp_communication_result_type = crt.skp_communication_result_type
where fcr.dtime_inserted >= trunc(sysdate-5) and fcr.skp_credit_case in (select skp_credit_case from ap_crm.cmp_tlc_fu_base) and ' || chr(10) || '(';
      if not (cfg_commlist%ISOPEN) then
            open cfg_commlist;
      end if;
      loop
      fetch cfg_commlist into SKP_COMM_CHANNEL, SKP_COMM_TYPE, SKP_COMM_SUBTYPE, SKP_COMM_SUBTYP_SPEC, SKP_COMM_SUBTYP_SUB_SPEC, SKP_COMM_STATUS, SKP_COMM_RESULT_TYPE;
      exit when cfg_commlist%NOTFOUND;
           sql_string :=  sql_string || case when substr(sql_string, -1, 1) = '(' then '(' else ' or(' end;
           if(SKP_COMM_CHANNEL IS NOT null) then
                sql_string := sql_string || case when substr(sql_string,-1,1) <> '(' then ' AND ' else '' end || 'fcr.SKP_COMMUNICATION_CHANNEL = ' || SKP_COMM_CHANNEL;
           END IF;
           if(SKP_COMM_TYPE IS NOT null) then
                sql_string := sql_string || case when substr(sql_string,-1,1) <> '(' then ' AND ' else '' end || 'fcr.SKP_COMMUNICATION_TYPE = ' || SKP_COMM_TYPE;
           END IF;
           if(SKP_COMM_SUBTYPE IS NOT null) then
                sql_string := sql_string || case when substr(sql_string,-1,1) <> '(' then ' AND ' else '' end || 'fcr.SKP_COMMUNICATION_SUBTYPE = ' || SKP_COMM_SUBTYPE;
           END IF;
           if(SKP_COMM_SUBTYP_SPEC IS NOT null) then
                sql_string := sql_string || case when substr(sql_string,-1,1) <> '(' then ' AND ' else '' end || 'fcr.SKP_COMM_SUBTYPE_SPECIF = ' || SKP_COMM_SUBTYP_SPEC;
           END IF;
           if(SKP_COMM_SUBTYP_SUB_SPEC IS NOT null) then
                sql_string := sql_string || case when substr(sql_string,-1,1) <> '(' then ' AND ' else '' end || 'fcr.SKP_COMM_SUBTYPE_SUB_SPECIF = ' || SKP_COMM_SUBTYP_SUB_SPEC;
           END IF;
           if(SKP_COMM_STATUS IS NOT null) then
                sql_string := sql_string || case when substr(sql_string,-1,1) <> '(' then ' AND ' else '' end || 'fcr.SKP_COMMUNICATION_STATUS = ' || SKP_COMM_STATUS;
           END IF;
           if(SKP_COMM_RESULT_TYPE IS NOT null) then
                sql_string := sql_string || case when substr(sql_string,-1,1) <> '(' then ' AND ' else '' end || 'fcr.SKP_COMMUNICATION_RESULT_TYPE = ' || SKP_COMM_RESULT_TYPE;
           END IF;
           sql_string := sql_string || ')' || chr(10);
      end loop;
      close cfg_commlist;
--      sql_string := sql_string || ');';
      sql_string := sql_string || ');' || chr(10) || 'commit; end;';
--      dbms_output.put_line(sql_string);
      AP_PUBLIC.CORE_LOG_PKG.pStart('Extract Communication Record');
      execute immediate sql_string;
      AP_PUBLIC.CORE_LOG_PKG.pEnd;
      pstats('gtt_cmp_rtn_02_commlist');

      -- Merge raw communication with existing data in AP_CRM
      pstats('camp_comm_rec_ob');
      AP_PUBLIC.CORE_LOG_PKG.pStart('Merge Outbound Communication Record');
      merge /*+ APPEND PARALLEL(4) */ into camp_comm_rec_ob tgt
      using
      (
          select date_call, dtime_inserted, skf_communication_record, skp_client, skp_credit_case,
                 skp_communication_channel, code_channel, name_communication_channel,
                 skp_communication_type, code_type_code, name_communication_type,
                 skp_communication_subtype, code_subtype, name_communication_subtype,
                 skp_Comm_subtype_specif, code_comm_subtype_specif, name_comm_subtype_specif,
                 skp_Comm_subtype_sub_specif, code_comm_subtype_sub_specif, name_comm_subtype_sub_specif,
                 skp_communication_status, code_status, name_communication_status,
                 skp_communication_result_type, code_result_type, name_communication_result_type,
                 text_note, text_contact, employee_number, common_name
          from gtt_cmp_rtn_02_commlist where skp_communication_channel in (1, 3, 4, 9, 3501)
      )src on (tgt.skf_communication_record = src.skf_communication_record and tgt.skp_client = src.skp_client)
      when not matched then insert
      (
           date_call, dtime_inserted, skf_communication_record, skp_client, skp_credit_case,
           skp_communication_channel, code_channel, name_communication_channel,
           skp_communication_type, code_type_code, name_communication_type,
           skp_communication_subtype, code_subtype, name_communication_subtype,
           skp_Comm_subtype_specif, code_comm_subtype_specif, name_comm_subtype_specif,
           skp_Comm_subtype_sub_specif, code_comm_subtype_sub_specif, name_comm_subtype_sub_specif,
           skp_communication_status, code_status, name_communication_status,
           skp_communication_result_type, code_result_type, name_communication_result_type,
           text_note, text_contact, employee_number, common_name
      )
      values
      (
           src.date_call, src.dtime_inserted, src.skf_communication_record, src.skp_client, src.skp_credit_case,
           src.skp_communication_channel, src.code_channel, src.name_communication_channel,
           src.skp_communication_type, src.code_type_code, src.name_communication_type,
           src.skp_communication_subtype, src.code_subtype, src.name_communication_subtype,
           src.skp_Comm_subtype_specif, src.code_comm_subtype_specif, src.name_comm_subtype_specif,
           src.skp_Comm_subtype_sub_specif, src.code_comm_subtype_sub_specif, src.name_comm_subtype_sub_specif,
           src.skp_communication_status, src.code_status, src.name_communication_status,
           src.skp_communication_result_type, src.code_result_type, src.name_communication_result_type,
           src.text_note, src.text_contact, src.employee_number, src.common_name
      );
      AP_PUBLIC.CORE_LOG_PKG.pEnd;
      commit;
      pstats('camp_comm_rec_ob');

      --Merge raw communication with existing data in AP_CRM
      pstats('camp_comm_rec_ib');
      AP_PUBLIC.CORE_LOG_PKG.pStart('Merge Inbound Communication Record');
      merge /*+ APPEND PARALLEL(4) */ into camp_comm_rec_ib tgt
      using
      (
          select date_call, dtime_inserted, skf_communication_record, skp_client, skp_credit_case,
                 skp_communication_channel, code_channel, name_communication_channel,
                 skp_communication_type, code_type_code, name_communication_type,
                 skp_communication_subtype, code_subtype, name_communication_subtype,
                 skp_Comm_subtype_specif, code_comm_subtype_specif, name_comm_subtype_specif,
                 skp_Comm_subtype_sub_specif, code_comm_subtype_sub_specif, name_comm_subtype_sub_specif,
                 skp_communication_status, code_status, name_communication_status,
                 skp_communication_result_type, code_result_type, name_communication_result_type,
                 text_note, text_contact, employee_number, common_name
          from gtt_cmp_rtn_02_commlist where skp_communication_channel in (5, 7, 8, 6, 107, 13501, 108, 301, 2)
      )src on (tgt.skf_communication_record = src.skf_communication_record)
      when not matched then insert
      (
           date_call, dtime_inserted, skf_communication_record, skp_client, skp_credit_case,
           skp_communication_channel, code_channel, name_communication_channel,
           skp_communication_type, code_type_code, name_communication_type,
           skp_communication_subtype, code_subtype, name_communication_subtype,
           skp_Comm_subtype_specif, code_comm_subtype_specif, name_comm_subtype_specif,
           skp_Comm_subtype_sub_specif, code_comm_subtype_sub_specif, name_comm_subtype_sub_specif,
           skp_communication_status, code_status, name_communication_status,
           skp_communication_result_type, code_result_type, name_communication_result_type,
           text_note, text_contact, employee_number, common_name
      )
      values
      (
           src.date_call, src.dtime_inserted, src.skf_communication_record, src.skp_client, src.skp_credit_case,
           src.skp_communication_channel, src.code_channel, src.name_communication_channel,
           src.skp_communication_type, src.code_type_code, src.name_communication_type,
           src.skp_communication_subtype, src.code_subtype, src.name_communication_subtype,
           src.skp_Comm_subtype_specif, src.code_comm_subtype_specif, src.name_comm_subtype_specif,
           src.skp_Comm_subtype_sub_specif, src.code_comm_subtype_sub_specif, src.name_comm_subtype_sub_specif,
           src.skp_communication_status, src.code_status, src.name_communication_status,
           src.skp_communication_result_type, src.code_result_type, src.name_communication_result_type,
           src.text_note, src.text_contact, src.employee_number, src.common_name
      );
      AP_PUBLIC.CORE_LOG_PKG.pEnd;
      commit;
      pstats('camp_comm_rec_ib');

      -- Retrieve Communication Result Part for the extracted communication record
      pstats('camp_comm_res_part');
      AP_PUBLIC.CORE_LOG_PKG.pStart('Merge Communication Result Parts');
      merge /*+ PARALLEL(4) */ into camp_comm_res_part tgt
      using
      (
            select skf_comm_result_part, skf_communication_record, code_comm_result_part, text_value, dtime_inserted from owner_Dwh.f_Comm_Result_Part_Tt
            where skf_communication_record in (select skf_communication_record from gtt_cmp_rtn_02_commlist) and flag_archived = 'N' and flag_deleted = 'N'
      )src on (tgt.skf_comm_result_part = src.skf_comm_result_part)
      when not matched then insert
      (
         skf_comm_result_part, skf_communication_record, code_comm_result_part, text_value, dtime_inserted
      )
      values
      (
         src.skf_comm_result_part, src.skf_communication_record, src.code_comm_result_part, src.text_value, src.dtime_inserted
      );
      AP_PUBLIC.CORE_LOG_PKG.pEnd;
      commit;
      pstats('camp_comm_res_part');
      AP_PUBLIC.CORE_LOG_PKG.pFinish;
end;
/

