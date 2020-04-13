select * from sv_bus.service_history where general_1='10210376'
select * from sv_bus.service_history where service_name='014026861'

select * from sv_bus.product_instance_history where general_1='10469519' and base_product_instance_id is  null
select * from sv_bus.product_instance_history where base_product_instance_id in (2426157, 2426165, 2426143, 2426150)