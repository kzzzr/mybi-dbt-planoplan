SELECT DISTINCT 

	  halfMD5(ga_devicecategory, ga_browserversion, ga_operatingsystem, ga_operatingsystemversion) AS device_id
	, ga_devicecategory
	, ga_browserversion	
	, ga_operatingsystem
	, ga_operatingsystemversion

FROM {{ ref('stg_devices') }}