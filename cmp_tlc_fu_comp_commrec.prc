create or replace procedure cmp_tlc_fu_comp_commrec is
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
      AP_PUBLIC.CORE_LOG_PKG.pInit( 'AP_CRM', 'CMP_TLC_FU_COMP_COMMREC');
      ptruncate('gtt_cmp_rtn_02_commlist');
      pstats('gtt_cmp_rtn_02_commlist');
      AP_PUBLIC.CORE_LOG_PKG.pStart('Compile Communication Record');
      insert /*+ APPEND */ into gtt_cmp_rtn_02_commlist
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
      FROM ap_crm.camp_comm_rec_ob fcr
      left join owner_Dwh.dc_credit_case dcc on fcr.skp_credit_case = dcc.skp_credit_case
      left join owner_dwh.cl_communication_channel ccl on fcr.skp_communication_channel = ccl.skp_communication_channel
      left join owner_dwh.cl_communication_type cct on fcr.skp_communication_type = cct.SKP_COMMUNICATION_TYPE
      left join owner_dwh.cl_communication_subtype ccs on fcr.skp_communication_subtype = ccs.skp_communication_subtype
      left join owner_dwh.cl_comm_subtype_specif css on fcr.skp_comm_subtype_specif = css.SKP_COMM_SUBTYPE_SPECIF
      left join owner_dwh.cl_comm_subtype_sub_specif csf on fcr.skp_comm_subtype_sub_specif = csf.SKP_COMM_SUBTYPE_SUB_SPECIF
      left join owner_dwh.cl_communication_status cst on fcr.skp_communication_status = cst.skp_communication_status
      left join owner_Dwh.cl_communication_result_type crt on fcr.skp_communication_result_type = crt.skp_communication_result_type
      where fcr.skp_credit_case in (select skp_credit_case from cmp_tlc_fu_base)
      union all
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
      FROM ap_crm.camp_comm_rec_ib fcr
      left join owner_Dwh.dc_credit_case dcc on fcr.skp_credit_case = dcc.skp_credit_case
      left join owner_dwh.cl_communication_channel ccl on fcr.skp_communication_channel = ccl.skp_communication_channel
      left join owner_dwh.cl_communication_type cct on fcr.skp_communication_type = cct.SKP_COMMUNICATION_TYPE
      left join owner_dwh.cl_communication_subtype ccs on fcr.skp_communication_subtype = ccs.skp_communication_subtype
      left join owner_dwh.cl_comm_subtype_specif css on fcr.skp_comm_subtype_specif = css.SKP_COMM_SUBTYPE_SPECIF
      left join owner_dwh.cl_comm_subtype_sub_specif csf on fcr.skp_comm_subtype_sub_specif = csf.SKP_COMM_SUBTYPE_SUB_SPECIF
      left join owner_dwh.cl_communication_status cst on fcr.skp_communication_status = cst.skp_communication_status
      left join owner_Dwh.cl_communication_result_type crt on fcr.skp_communication_result_type = crt.skp_communication_result_type
      where fcr.skp_credit_case in (select skp_credit_case from cmp_tlc_fu_base);
      AP_PUBLIC.CORE_LOG_PKG.pEnd;
      commit;
      pstats('gtt_cmp_rtn_02_commlist');

      -- Transpose communication result part into dataset
      ptruncate('GTT_CMP_TLC_COMM_PART');
      AP_PUBLIC.CORE_LOG_PKG.pStart('Compile Communication Result Parts');
      insert /*+ APPEND */ into GTT_CMP_TLC_COMM_PART
      select skf_communication_record,
             coalesce(max(decode(code_comm_result_part, 'PRMS_DATE_TELCO', text_value)), max(decode(code_comm_result_part, 'PRMS_DT_MPF', text_value)))DATE_PROMISE,
             max(decode(code_comm_result_part, 'SA_PHNNUM', text_value)) SA_PHONE,
             max(decode(code_comm_result_part, 'SA_MNAME_TELCO', text_value)) SA_NAME,
             max(decode(code_comm_result_part, 'POS_NAME_TELCO', text_value)) POS_NAME,
             max(decode(code_comm_result_part, 'SA_MAILADD_TELCO', text_value))SA_EMAIL,
             max(decode(code_comm_result_part, 'POS_CODE_TELCO', text_value))POS_CODE,
             max(decode(code_comm_result_part, 'SA_CODE_TELCO', text_value)) SA_CODE
      from camp_comm_res_part
      where skf_communication_record in (select skf_communication_record from ap_crm.gtt_cmp_rtn_02_commlist)
      group by skf_communication_record;
      AP_PUBLIC.CORE_LOG_PKG.pEnd;
      commit;
      pstats('GTT_CMP_TLC_COMM_PART');

      ptruncate('cmp_tlc_fu_comm_list');
      pstats('cmp_tlc_fu_comm_list');
      AP_PUBLIC.CORE_LOG_PKG.pStart('Compile Final Communication Result');
      insert /*+ APPEND */ into cmp_tlc_fu_comm_list
      with comm as
      (
         select * from gtt_cmp_rtn_02_commlist
         where (skp_client, skp_credit_case, dtime_inserted) in
         (
             select skp_client, skp_credit_case, max(dtime_inserted)dtime_inserted from gtt_cmp_rtn_02_commlist
             group by skp_client, skp_credit_case
         )
      ),
      contact as
      (
          select con.skp_credit_case, con.text_contact, cct.NAME_CONTACT_TYPE from owner_Dwh.f_Credit_Case_Request_Cont_Tt con
          left join owner_dwh.cl_contact_type cct on con.skp_contact_type = cct.SKP_CONTACT_TYPE
          where skp_credit_case in (select skp_credit_case from comm)
            and con.skp_contact_type = 2     
      ),
      comp as
      (
          select /*+ MATERIALIZE */ comm.date_call, comm.skf_communication_record, comm.skp_client, comm.skp_credit_case,
                 comm.name_communication_channel, comm.name_communication_type, comm.name_communication_subtype, comm.name_comm_subtype_specif, comm.name_comm_subtype_sub_specif, comm.name_communication_status, comm.name_communication_result_type,
                 comm.text_note, comm.text_contact,
                 substr(max(nvl(ccl.name_sub_campaign,'1.General')),3)sub_campaign,
                 case when substr(max(nvl(ccl.action,'1.Call')),3) = 'Call' then
                          case when substr(max(nvl(ccl.ignore_callback,'2.N')),3) = 'N' and crp.date_promise is not null and to_Date(substr(crp.date_promise,0,10),'dd/mm/yyyy') + max(ccl.delay_days) < trunc(sysdate) then 'Call'
                               when substr(max(nvl(ccl.ignore_callback,'2.N')),3) = 'N' and crp.date_promise is not null and to_Date(substr(crp.date_promise,0,10),'dd/mm/yyyy') + max(ccl.delay_days) >= trunc(sysdate) then 'Do not call'
                          else substr(max(ccl.action),3) end
                      when substr(max(nvl(ccl.action,'1.Call')),3) = 'Delay' then
                          case when substr(max(nvl(ccl.ignore_callback,'2.N')),3) = 'N' and crp.date_promise is not null and to_Date(substr(crp.date_promise,0,10),'dd/mm/yyyy') + max(ccl.delay_days) < trunc(sysdate) then 'Call' 
                               when substr(max(nvl(ccl.ignore_callback,'2.N')),3) = 'N' and crp.date_promise is not null and to_Date(substr(crp.date_promise,0,10),'dd/mm/yyyy') + max(ccl.delay_days) >= trunc(sysdate) then 'Do not call'
                               when substr(max(nvl(ccl.ignore_callback,'2.N')),3) = 'Y' and comm.date_call + max(ccl.delay_days) < trunc(sysdate) then 'Call'
                               when substr(max(nvl(ccl.ignore_callback,'2.N')),3) = 'Y' and comm.date_call + max(ccl.delay_days) >= trunc(sysdate) then 'Do not call'
                          else substr(max(ccl.action),3) end         
                 else 'Do not call' end action,
                 crp.date_promise, crp.sa_code, crp.sa_name, crp.sa_phone, crp.sa_email, crp.pos_name, crp.pos_code
          from comm
          left join camp_cfg_comm_list ccl  on ccl.name_campaign = 'Telco FU' and ccl.active = 'Y'
               and comm.skp_communication_channel = case when ccl.skp_communication_channel is null then comm.skp_communication_channel else ccl.skp_communication_channel end
               and comm.skp_communication_type = case when ccl.skp_communication_type is null then comm.skp_communication_type else ccl.skp_communication_type end
               and comm.skp_communication_subtype = case when ccl.skp_communication_subtype is null then comm.skp_communication_subtype else ccl.skp_communication_subtype end
               and comm.skp_comm_subtype_specif = case when ccl.skp_comm_subtype_specif is null then comm.skp_comm_subtype_specif else  ccl.skp_comm_subtype_specif end
               and comm.skp_comm_subtype_sub_specif = case when ccl.skp_comm_subtype_sub_specif is null then comm.skp_comm_subtype_sub_specif else ccl.skp_comm_subtype_sub_specif end
               and comm.skp_communication_result_type = case when ccl.skp_communication_result_type is null then comm.skp_communication_result_type else ccl.skp_communication_result_type end
          left join GTT_CMP_TLC_COMM_PART crp on comm.skf_communication_record = crp.skf_communication_record
          group by comm.date_call, comm.skf_communication_record, comm.skp_client, comm.skp_credit_case,
                 comm.name_communication_channel, comm.name_communication_type, comm.name_communication_subtype, comm.name_comm_subtype_specif, comm.name_comm_subtype_sub_specif, comm.name_communication_status, comm.name_communication_result_type,
                 comm.text_note, comm.text_contact, crp.date_promise, crp.sa_code, crp.sa_name, crp.sa_phone, crp.sa_email, crp.pos_name, crp.pos_code
      )
      select comp.date_call, comp.skf_communication_record, comp.skp_client, comp.skp_credit_case,
                 comp.name_communication_channel, comp.name_communication_type, comp.name_communication_subtype, comp.name_comm_subtype_specif, comp.name_comm_subtype_sub_specif, comp.name_communication_status, comp.name_communication_result_type,
                 comp.text_note, 
                 case when comp.name_communication_channel = 'Customer visit POS' and comp.name_communication_result_type = 'Visit Reschedule' then contact.text_contact else comp.text_contact end text_contact,
                 comp.sub_campaign,
                 comp.action,
                 comp.date_promise, comp.sa_code, comp.sa_name, comp.sa_phone, comp.sa_email, comp.pos_name, comp.pos_code
      from comp
      left join contact on comp.skp_credit_case = contact.skp_credit_case;
      AP_PUBLIC.CORE_LOG_PKG.pEnd;
      commit;
      pstats('cmp_tlc_fu_comm_list');
      AP_PUBLIC.CORE_LOG_PKG.pFinish;
end;
/

