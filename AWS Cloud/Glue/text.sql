MERGE INTO glue_catalog.iceberg_database_qa.incident_inquiry2 t
    USING (SELECT op,inc_id,inc_cnt_id_acknowledged_by,inc_affected_end_user,inc_applicant_id,inc_cnt_id_assigned_to,inc_callback_method,inc_change_type,inc_date_created,inc_department,inc_descr,inc_cnt_id_opened_by,inc_priority,inc_record_status,inc_resolved_date,inc_resolved_time,inc_resolved_time_taken,inc_response_time,inc_severity,inc_sla_id,inc_status,inc_status_1,inc_status_2,inc_status_3,inc_summary,application_id,appointment_id,inc_assigned_to,customer_id,field_json,inc_category,inc_subcategory,incident_display_id,inc_incoming_type,last_modified,last_modified_by,mission_id,next_sla_id,parent_sla_id,post_user_id,role_id,sla_value,user_role,applicant_full_name,dossier_number,ds_160_number,assign_type,user_id,priority_queue,incident_rule_type ,visa_category ,visa_class ,visa_type ,created_by_id ,created_dt ,modified_by_id ,modified_dt ,country_code
    FROM incremental_input_data1) as s 
    ON t.inc_id = s.inc_id
    WHEN MATCHED THEN UPDATE SET t.inc_id=s.inc_id,t.inc_cnt_id_acknowledged_by=s.inc_cnt_id_acknowledged_by,t.inc_affected_end_user=s.inc_affected_end_user,
    t.inc_applicant_id=s.inc_applicant_id,t.inc_cnt_id_assigned_to=s.inc_cnt_id_assigned_to,t.inc_callback_method=s.inc_callback_method,
    t.inc_change_type=s.inc_change_type,t.inc_date_created=s.inc_date_created,t.inc_department=s.inc_department,t.inc_descr=s.inc_descr,
    t.inc_cnt_id_opened_by=s.inc_cnt_id_opened_by,t.inc_priority=s.inc_priority,t.inc_record_status=s.inc_record_status,
    t.inc_resolved_date=s.inc_resolved_date,t.inc_resolved_time=s.inc_resolved_time,t.inc_resolved_time_taken=s.inc_resolved_time_taken,
    t.inc_response_time=s.inc_response_time,t.inc_severity=s.inc_severity,t.inc_sla_id=s.inc_sla_id,t.inc_status=s.inc_status,
    t.inc_status_1=s.inc_status_1,t.inc_status_2=s.inc_status_2,t.inc_status_3=s.inc_status_3,t.inc_summary=s.inc_summary,
    t.application_id=s.application_id,t.appointment_id=s.appointment_id,t.inc_assigned_to=s.inc_assigned_to,t.customer_id=s.customer_id,
    t.field_json=s.field_json,t.inc_category=s.inc_category,t.inc_subcategory=s.inc_subcategory,t.incident_display_id=s.incident_display_id,
    t.inc_incoming_type=s.inc_incoming_type,t.last_modified=s.last_modified,t.last_modified_by=s.last_modified_by,t.mission_id=s.mission_id,
    t.next_sla_id=s.next_sla_id,t.parent_sla_id=s.parent_sla_id,t.post_user_id=s.post_user_id,t.role_id=s.role_id,t.sla_value=s.sla_value,
    t.user_role=s.user_role,t.applicant_full_name=s.applicant_full_name,t.dossier_number=s.dossier_number,t.ds_160_number=s.ds_160_number,
    t.assign_type=s.assign_type,t.user_id=s.user_id,t.priority_queue=s.priority_queue,t.incident_rule_type =s.incident_rule_type,t.visa_category = s.visa_category,
    t.visa_class = s.visa_class,t.visa_type =s.visa_type,t.created_by_id = s.created_by_id,t.created_dt = s.created_dt,t.modified_by_id = s.modified_by_id,
    t.modified_dt=s.modified_dt,t.country_code=s.country_code

    WHEN NOT MATCHED THEN INSERT (inc_id,inc_cnt_id_acknowledged_by,inc_affected_end_user,inc_applicant_id,inc_cnt_id_assigned_to,inc_callback_method,inc_change_type,inc_date_created,inc_department,inc_descr,inc_cnt_id_opened_by,inc_priority,inc_record_status,inc_resolved_date,inc_resolved_time,inc_resolved_time_taken,inc_response_time,inc_severity,inc_sla_id,inc_status,inc_status_1,inc_status_2,inc_status_3,inc_summary,application_id,appointment_id,inc_assigned_to,customer_id,field_json,inc_category,inc_subcategory,incident_display_id,inc_incoming_type,last_modified,last_modified_by,mission_id,next_sla_id,parent_sla_id,post_user_id,role_id,sla_value,user_role,applicant_full_name,dossier_number,ds_160_number,assign_type,user_id,priority_queue,incident_rule_type,visa_category,visa_class,visa_type,created_by_id,created_dt,modified_by_id,modified_dt,country_code) 
	VALUES (s.inc_id,s.inc_cnt_id_acknowledged_by,s.inc_affected_end_user,s.inc_applicant_id,s.inc_cnt_id_assigned_to,s.inc_callback_method,
        s.inc_change_type,s.inc_date_created,s.inc_department,s.inc_descr,s.inc_cnt_id_opened_by,s.inc_priority,s.inc_record_status,
        s.inc_resolved_date,s.inc_resolved_time,s.inc_resolved_time_taken,s.inc_response_time,s.inc_severity,s.inc_sla_id,s.inc_status,
        s.inc_status_1,s.inc_status_2,s.inc_status_3,s.inc_summary,s.application_id,s.appointment_id,s.inc_assigned_to,s.customer_id,
        s.field_json,s.inc_category,s.inc_subcategory,s.incident_display_id,s.inc_incoming_type,s.last_modified,s.last_modified_by,
        s.mission_id,s.next_sla_id,s.parent_sla_id,s.post_user_id,s.role_id,s.sla_value,s.user_role,s.applicant_full_name,s.dossier_number,
        s.ds_160_number,s.assign_type,s.user_id,s.priority_queue,s.incident_rule_type,s.visa_category,s.visa_class,s.visa_type,s.created_by_id,s.created_dt,s.modified_by_id,s.modified_dt,s.country_code)
