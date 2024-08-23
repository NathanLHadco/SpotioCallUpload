USE [GDB_01_001]
GO
/****** Object:  StoredProcedure [dbo].[Hadco_Spotio_CallUpload]    Script Date: 8/21/2024 10:22:46 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[Hadco_Spotio_CallUpload]
AS

DECLARE @command nvarchar(max), @cmdquery varchar(8000)
DECLARE @filename varchar(100), @LotsOfText nvarchar(max), @xml xml

BEGIN
	
--Get a list of files in the app directory
	DECLARE @filelist table
	(
		id int identity(1,1)
		,subdirectory nvarchar(512)
		,depth int
		,isfile bit
	)
	INSERT INTO @filelist (subdirectory, depth, isfile)
	EXEC xp_dirtree 'D:\CallReportFiles\spotio', 1, 1

	DECLARE @counter int, @max int
	SET @counter = 1
	SELECT @max = COUNT(*) FROM @filelist  WHERE isfile = 1
	IF @max > 0
	BEGIN

		WHILE @counter <= @max
		BEGIN
			SELECT @filename = subdirectory 
			FROM @filelist 
			WHERE id = @counter
			AND isfile = 1
			--Get file xml
			SET @command =
				'BEGIN TRY 
					SELECT @LotsOfText = CONVERT(nvarchar(max), CONVERT(xml, BulkColumn)) FROM OPENROWSET(  
					BULK ''D:\CallReportFiles\spotio\' + @filename + ''',  
					SINGLE_BLOB)d; 
				END TRY 
				BEGIN CATCH 
					SELECT @LotsOfText = ''Error'';
				END CATCH;'
			EXECUTE sp_ExecuteSQL @command, N'@LotsOfText nvarchar(max) output ', @LotsOfText output


			DECLARE @spotio_id varchar(100), @account_type varchar(5), @name varchar(60), @user varchar(10), @insideUser varchar(10), @date datetime, @address varchar(1000), @acctno varchar(20), @stageid varchar(20), @email varchar(100), @website varchar(100), @tel varchar(100), @approved varchar(8), @OSFocus varchar(100), @username varchar(100)
			IF @LotsOfText <> 'Error'

			BEGIN
				SELECT @xml = CONVERT(xml, @LotsOfText)

				--Clear variables
				SET @date = GETDATE()
				SET @spotio_id = ''
				SET @name = '' 
				SET @user = ''
				SET @address = ''
				SET @acctno = ''
				SET @stageid = ''
	--nate file variable
				DECLARE @processedFile bit
				set @processedFile = 'false'

				--Get date
				SELECT @date = data.value('(date)[1]','DATETIME')
				FROM @xml.nodes('/ActivityOutput/ActivityItem') AS TEMPTABLE(data)
				--Get Spotio ID and name
				SELECT @acctno = ISNULL(data.value('(externalDataObjectId)[1]','VARCHAR(1000)'),'')
				FROM @xml.nodes('/ActivityOutput/CustomerData') AS TEMPTABLE(data)

				SELECT @spotio_id = ISNULL(data.value('(id)[1]','VARCHAR(100)'),'')
	--acctno has no value here
					
					, @name = RTRIM(REPLACE(ISNULL(data.value('(name)[1]','VARCHAR(100)'),''), RTRIM(@acctno), ''))
					, @address = ISNULL(data.value('(address)[1]','VARCHAR(1000)'),'')
					, @stageid = ISNULL(data.value('(stageId)[1]','VARCHAR(20)'),'')
				FROM @xml.nodes('/ActivityOutput/ActivityItem/dataObject') AS TEMPTABLE(data)

				-- Match salesperson based on user or name
				SELECT @user = 
				(
					SELECT ISNULL(MAX(CCODE),'AUTO') 
					FROM USERLIST 
					WHERE (RTRIM(HTTP_PROXY_USER) = REPLACE(data.value('(email)[1]','VARCHAR(100)'), '@hadco-metal.com','')
						OR (RTRIM(F_NAME) = data.value('(firstName)[1]','VARCHAR(100)')
							AND RTRIM(L_NAME) = data.value('(lastName)[1]','VARCHAR(100)')))
				)
				FROM @xml.nodes('/ActivityOutput/ReportingUser') AS TEMPTABLE(data)		 
				
					
				--Get customer field data (contacts, etc.)
				CREATE TABLE #TempFieldData 
				(
					fieldId INT,
					fieldValue VARCHAR(100)
				)
				INSERT INTO #TempFieldData (fieldId, fieldValue)
				SELECT data.value('(fieldId)[1]', 'INT'),
						data.value('(value)[1]', 'VARCHAR(100)')
				FROM @xml.nodes('/ActivityOutput/CustomerData/fields/Field') AS TEMPTABLE(data)

				set @website = (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 20)
				set @OSFocus = (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 14)
				set @account_type = (SELECT TOP 1 CS.ACCOUNT_TYPE FROM CUSTVENDSETUP CS JOIN TBLCODE T ON CS.ACCOUNT_TYPE = T.TBLCODE AND TBLTYPE = '002' where name = (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 10))

				--Get details about the new customer
					
					SELECT @tel = data.value('(string)[1]', 'VARCHAR(100)')
					FROM @xml.nodes('/ActivityOutput/CustomerData/phones') AS TEMPTABLE(data)

					
					SELECT @email = data.value('(string)[1]', 'VARCHAR(100)')
					FROM @xml.nodes('/ActivityOutput/CustomerData/emails') AS TEMPTABLE(data)

					--Parse address
					if @address <> ''
					BEGIN
						DECLARE @addr1 varchar(50), @city varchar(30), @state varchar(4), @zip varchar(12)
						DECLARE @comma1 int, @comma2 int, @comma3 int, @substr2 varchar(1000), @substr3 varchar(1000)
						SELECT @comma1 = CHARINDEX(',', @address)
						SELECT @substr2 = RIGHT(@address, LEN(@address) - @comma1)
						SELECT @comma2 = CHARINDEX(',', @substr2)
						SELECT @substr3 = RIGHT(@substr2, LEN(@substr2) - @comma2)
						SELECT @comma3 = CHARINDEX(',', @substr3)

						SELECT @addr1 = LEFT(@address, @comma1 - 1)
						SELECT @city = LTRIM(LEFT(@substr2, @comma2 - 1))
						SELECT @state = LEFT(LTRIM(LEFT(@substr3, @comma3 - 1)),2)
						SELECT @zip = REPLACE(LTRIM(LEFT(@substr3, @comma3 - 1)), @state + ' ', '')
					END

				-- help us clean our old connections, necessary to update call history from old accts
				If @acctno IN (SELECT ACCTNO FROM CUSTVEND) AND @spotio_id NOT IN (SELECT spotio_id FROM Hadco_SpotIO_ID)
				BEGIN
				INSERT INTO Hadco_SpotIO_ID (acctno, spotio_id)
				VALUES (@acctno, @spotio_id)
				END
				
				--since we linked acctno and spotio id above, we only need to search by hadco spotio id
				If @name <> '' AND @spotio_id NOT IN (SELECT spotio_id FROM Hadco_SpotIO_ID)
				BEGIN
					


					--Much of this section borrowed from uspAddNewCustumer, from the old Call Report app
					DECLARE @next int
					select @next=cust_count from compsetup1
					set @next=@next+1
					update compsetup1 set cust_count=cust_count+1
					SET @acctno = CAST(@next AS varchar(12))

					--Insert customer
					insert into "CUSTVEND"
						("ACCTNO", "SUBC", "CUST_VEND","REGION","NAME","ADR1","ADR2","CITY","STATE","ZIP","COUNTRY","WEB_SITE","TEL1","EMAIL","FAX_TIME", "ALT1_CODE_C", "ALT2_CODE_C", "ALT3_CODE_C", "ALT4_CODE_C", "PRINT_TARGET", "PRINT_EMAIL_FORMAT", "INTERCOMPANY_ACCOUNT", "ADDED_USR", "ADDED_DTE")
					VALUES
					(                             
						@acctno
						, '1'
						, 'C'
						, @user
						, @name
						, @addr1                                                                                                                                             --addr1 = everything before the first comma
						, ''
						, @city
						, @state
						, @zip
						, 'US'
						, (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 20) --website
						, @tel
						, @email
						,'00:00'
						, NULL
						, NULL
						, NULL
						, ''
						, '2'
						, '1'
						, 'N'
						, @user
						, getdate()
					)

					insert into "CUSTVENDSETUP"
						("ACCTNO", "SUBC", "CUST_VEND", "PRIORITY", "ACCOUNT_TYPE", "RCV_OVER_SHIP", "SHIP_DEF_NO", "BILL_DEF_NO", "PAY_DEF_NO", "PAY_US", "COMPANYNO", "DIVISION", "DEPART", "FORM_1099", "HOLD", "HOLD_DATE", "HOLD_BY", "APPROVED", "APPROVED_DATE", "APPROVED_BY", "ULINES","TERM_CODE" ,"SHIP_COMPLETE", "POST_SHIP_DOC", "INV_TYPE", "ACCOUNT_RATE", "EARLY_SHIPMENT", "DAYS_BEFORE_SHIPPING", "FOB", "FOB_LBL", "GL_ACCOUNT", "CURENCY_CONV", "MIN_ORDER_LINE", "MIN_ORDER", "QT_TYPE_DEF", "SO_TYPE_DEF", "RF_TYPE_DEF", "PO_TYPE_DEF", "SMAN1_NG", "SMAN2_NG", "SMAN3_NG", "SMAN4_NG", "SMAN5_NG", "SMAN1_DOCLN", "SMAN2_DOCLN", "SMAN3_DOCLN", "SMAN4_DOCLN", "SMAN5_DOCLN", "DISC_TYPE", "DISC_DOCLN", "MISC1_DOCLN", "MISC2_DOCLN", "MISC3_DOCLN", "MISC4_DOCLN", "MISC5_DOCLN", "MISC6_DOCLN", "MISC1_TYPE", "MISC2_TYPE", "MISC3_TYPE", "MISC4_TYPE", "MISC5_TYPE", "MISC6_TYPE", "MISC1_PRN", "MISC2_PRN", "MISC3_PRN", "MISC4_PRN", "MISC5_PRN", "MISC6_PRN", "MISC1_TTL", "MISC2_TTL", "MISC3_TTL", "MISC4_TTL", "MISC5_TTL", "MISC6_TTL", "SUBT_TAX_A", "SUBT_TAX_B", "SUBT_TAX_C", "M1_TAX_A", "M1_TAX_B", "M1_TAX_C", "M2_TAX_A", "M2_TAX_B", "M2_TAX_C", "M3_TAX_A", "M3_TAX_B", "M3_TAX_C", "M4_TAX_A", "M4_TAX_B", "M4_TAX_C", "M5_TAX_A", "M5_TAX_B", "M5_TAX_C", "M6_TAX_A", "M6_TAX_B", "M6_TAX_C", "TAX_A_CODE", "TAX_B_CODE", "TAX_C_CODE", "TAX_A_TTL", "TAX_B_TTL", "TAX_C_TTL", "EXCH_CORE_CHRG", "EXCH_CORE_RETURN", "EXCH_CORE_NOTE", "EXCH_CHARGE_FROM", "EXCH_CHARGE_COST", "EXCH_CORE_COST", "EXCH_CORE_PERC", "SMAN1_CODE", "ADDED_USR", "ADDED_DTE")
					VALUES
					(
						@acctno
						, '1'
						, 'C'
						, (CASE WHEN (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 14) = 'Y' THEN 'P' --High Priority Customer
								WHEN @stageid = '5' THEN 'X' --Not a Good Fit
								WHEN @stageid = '6' THEN 'Z' --Business Closed
								ELSE NULL
							END) 
						, (
								SELECT MIN(T.TBLCODE)
								FROM TBLCODE T
								JOIN #TempFieldData F ON T.NAME = F.fieldValue AND F.fieldID = 10
							) --Account Type
						, 1.500000000000000e+001
						, 0
						, 0
						, 0
						, '02L'
						, 1
						, NULL
						, NULL
						, 'N'
						, 'Y'
						, getdate()
						, @user
						, 'N'
						, getdate()
						, @user
						, 'N','RTBD', 'N', 'Y', 'D', NULL, 'N', 0, 'DST', 'FOB', '40000', 'USD', 0.000000000000000e+000, 4.000000000000000e+001, 'Q', 'S', 'R', 'P', 'N', 'N', 'N', 'N', 'N', 'D', 'D', 'D', 'D', 'D'
						, '0', 'D', 'D', 'D', 'D', 'D', 'D', 'D', '1', '1', '1', '1', '1', '1', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N', 'Y', 'N', 'N', 'Y', 'N', 'N', 'Y', 'N'
						, 'N', 'Y', 'N', 'N', 'Y', 'N', 'N', NULL, NULL, NULL, 'N', 'N', 'N', 'N', 30, 'Y', '2', '1', '5', 10
						, (
								SELECT MIN(U.CCODE)
								FROM USERLIST U
								JOIN #TempFieldData F 
								ON U.F_NAME = LEFT(F.fieldValue, CHARINDEX(' ', F.fieldValue) - 1)
									AND U.L_NAME = RIGHT(RTRIM(F.fieldValue), LEN(RTRIM(F.fieldValue)) - CHARINDEX(' ', F.fieldValue))
									AND F.fieldID = 21
							) --Inside Salesperson
						, @user
						, getdate()
					)

					--Record Pentagon acctno / SpotIO ID relationship
					INSERT INTO Hadco_SpotIO_ID (acctno, spotio_id)
					VALUES (@acctno, @spotio_id)

					--keep track if file was processed
					set @processedFile = 'true'
				END	

				
	--since we linked acctno and spotio id above, we only need to search by hadco spotio id
				
	
				IF @name <> '' AND @spotio_id IN (SELECT spotio_id FROM Hadco_SpotIO_ID)
				BEGIN

					----------------------------------------------------------
					--Update customer details
					----------------------------------------------------------

					UPDATE "CUSTVEND"
					set
					"WEB_SITE" = @website
					, "ALT3_CODE_C" = @OSFocus
					where "ACCTNO" = @acctno

					UPDATE "CUSTVENDSETUP"
					set
					"ACCOUNT_TYPE" = @account_type
					where "acctno" = @acctno

					set @approved = ''
					set @approved = (select APPROVED from CUSTVENDSETUP where @acctno = ACCTNO)

					If @acctno <> '' AND @approved = 'N'
						begin
				
						UPDATE "CUSTVEND"
						set
							"ADR1" = RTRIM(@addr1)
							,"CITY" = RTRIM(@city)
							,"STATE" = RTRIM(@state)
							,"ZIP" = RTRIM(@zip)
							,"NAME" = @name
						where "ACCTNO" = @acctno

						--Not a Good Fit
						if @stageid = '5'
						BEGIN
							UPDATE CUSTVENDSETUP
							SET PRIORITY = 'X'
							WHERE ACCTNO = @acctno
							AND CUST_VEND = 'C'
						END
					
						--Business Closed
						if @stageid = '6'
						BEGIN
							UPDATE CUSTVENDSETUP
							SET PRIORITY = 'Z'
							WHERE ACCTNO = @acctno
							AND CUST_VEND = 'C'
						END

					--keep track if file was processed
					set @processedFile = 'true'
				
					end

					----------------------------------------------------------
					--Update Priority
					----------------------------------------------------------
					SELECT @acctno = acctno
					FROM Hadco_SpotIO_ID
					WHERE spotio_id = @spotio_id


					----------------------------------------------------------
					--Update call tracking
					----------------------------------------------------------
					DECLARE @DocNo as nvarchar(20)

					--increment by 1 and get the doc no 
					UPDATE [GDB_01_001].[dbo].[COUNTERSTBL]
					SET  
						[COUNTER] = [COUNTER]+1         
					where DOC_CATEGORY='CT'

					select @DocNo= Counter from COUNTERSTBL
					where DOC_CATEGORY='CT'
					order by counter 

					--Get acctno
					SELECT @acctno = acctno
					FROM Hadco_SpotIO_ID
					WHERE spotio_id = @spotio_id

					INSERT INTO CALL_TRACKING
						([DOC_NO]
						,[DOC_CATEGORY]
						,[CALLTYPE]
						,[ENTERED_BY]
						,[ENTERED_DATE]
						,[CALL_DOC_NO]
						,[CALL_DOC_CATEGORY]
						,[CUST_VEND]
						,[ACCTNO]
						,[SUBC]
						,[CCODE]
						,[CATEGORY]
						,[CALL_MSG]
						,[REFERED_TO]
						,[CALL_BACK_ON]
						,[COMPLETED_ON]
						,[COMPLETED_BY]
						,[RECLOCK]
						,[ADDED_USR]
						,[ADDED_DTE]
						,[UPDATED_USR]
						,[UPDATED_DTE])
					SELECT
						@DocNo
						,'CT'
						,'0'
						,@user
						,@date
						,(SELECT ISNULL(MAX(CALL_DOC_NO),0) + 1 FROM CALL_TRACKING WHERE ACCTNO = @acctno)
						,'C'
						,'C'
						, @acctno
						,'1'
						,null
						--Activity Results List of values
						--1 = On-site visit - Contacted
						--2 = On-site visit - No Contact
						--20 = On-Site visit - High Priority
						--24 = Phone Call - Contacted
						--25 = Phone Call - No Contact
						--26 = Phone Call - High Priority
						, IIF(data.value('(resultId)[1]','INT') IN (24,25,26), 'PHONE', 'VISIT')
						, data.value('(notes)[1]','VARCHAR(8000)')
						,null
						,null
						,null
						,null
						,null
						,@user
						,getdate()
						,null
						,null
					FROM @xml.nodes('/ActivityOutput/ActivityItem') AS TEMPTABLE(data)

					--Add or update contacts, if appropriate
					DECLARE @doc_no int = 0

					--Contact 1
					--Update
					IF EXISTS (SELECT * FROM #TempFieldData WHERE fieldID IN (70,71))
						AND EXISTS (SELECT * FROM CONTACTS WHERE ACCTNO = @acctno AND CCODE = 'S01')
					BEGIN
						UPDATE CONTACTS
						SET F_NAME = (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 70) --first name
							, L_NAME = (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 71) --last name
							, TITLE = (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 72) --title
							, TEL1 = (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 74) --tel
							, EMAIL = (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 73) --email
							, UPDATED_USR = 'AUTO'
							, UPDATED_DTE = GETDATE()
						WHERE ACCTNO = @acctno
						AND CCODE = 'S01'
					END
					--Insert
					IF EXISTS (SELECT * FROM #TempFieldData WHERE fieldID IN (70,71))
						AND NOT EXISTS (SELECT * FROM CONTACTS WHERE ACCTNO = @acctno AND CCODE = 'S01')
					BEGIN
						--Get and increment counter	
						SELECT @doc_no = COUNTER FROM COUNTERSTBL WHERE DOC_CATEGORY = 'CDN' AND DOC_TYPE = 'D'
						UPDATE COUNTERSTBL SET COUNTER = COUNTER + 1 WHERE DOC_CATEGORY = 'CDN' AND DOC_TYPE = 'D'

						--Insert contact records
						INSERT INTO CONTACTS (DOC_NO, ACCTNO, SUBC, CCODE, F_NAME, L_NAME, TITLE, TEL1, EMAIL, HOLD, COMPANYNO, ADDED_DTE, ADDED_USR)
						SELECT
							@doc_no + 1
							, @acctno
							, '1'
							, 'S01'
							, (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 70) --first name
							, (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 71) --last name
							, (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 72) --title
							, (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 74) --tel
							, (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 73) --email
							, 'N'
							, 1
							, GETDATE()
							, 'AUTO'

						--Update default contact, if none exists
						UPDATE CUSTVENDSETUP
						SET CCODE = 'S01'
						WHERE ACCTNO = @acctno
						AND CUST_VEND = 'C'
						AND ISNULL(CCODE,'') = ''
					END

					--Contact 2
					--Update
					IF EXISTS (SELECT * FROM #TempFieldData WHERE fieldID IN (62,63))
						AND EXISTS (SELECT * FROM CONTACTS WHERE ACCTNO = @acctno AND CCODE = 'S02')
					BEGIN
						UPDATE CONTACTS
						SET F_NAME = (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 63) --first name
							, L_NAME = (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 62) --last name
							, TITLE = (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 59) --title
							, TEL1 = (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 61) --tel
							, EMAIL = (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 60) --email
							, UPDATED_USR = 'AUTO'
							, UPDATED_DTE = GETDATE()
						WHERE ACCTNO = @acctno
						AND CCODE = 'S02'
					END
					--Insert
					IF EXISTS (SELECT * FROM #TempFieldData WHERE fieldID IN (62,63))
						AND NOT EXISTS (SELECT * FROM CONTACTS WHERE ACCTNO = @acctno AND CCODE = 'S02')
					BEGIN
						--Get and increment counter	
						SET @doc_no = 0
						SELECT @doc_no = COUNTER FROM COUNTERSTBL WHERE DOC_CATEGORY = 'CDN' AND DOC_TYPE = 'D'
						UPDATE COUNTERSTBL SET COUNTER = COUNTER + 1 WHERE DOC_CATEGORY = 'CDN' AND DOC_TYPE = 'D'

						--Insert contact records
						INSERT INTO CONTACTS (DOC_NO, ACCTNO, SUBC, CCODE, F_NAME, L_NAME, TITLE, TEL1, EMAIL, HOLD, COMPANYNO, ADDED_DTE, ADDED_USR)
						SELECT
							@doc_no + 1
							, @acctno
							, '1'
							, 'S02'
							, (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 63) --first name
							, (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 62) --last name
							, (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 59) --title
							, (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 61) --tel
							, (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 60) --email
							, 'N'
							, 1
							, GETDATE()
							, 'AUTO'
					END

					--Contact 3
					--Update
					IF EXISTS (SELECT * FROM #TempFieldData WHERE fieldID IN (56,57))
						AND EXISTS (SELECT * FROM CONTACTS WHERE ACCTNO = @acctno AND CCODE = 'S03')
					BEGIN
						UPDATE CONTACTS
						SET F_NAME = (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 57) --first name
							, L_NAME = (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 56) --last name
							, TITLE = (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 55) --title
							, TEL1 = (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 54) --tel
							, EMAIL = (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 53) --email
							, UPDATED_USR = 'AUTO'
							, UPDATED_DTE = GETDATE()
						WHERE ACCTNO = @acctno
						AND CCODE = 'S03'
					END
					--Insert
					IF EXISTS (SELECT * FROM #TempFieldData WHERE fieldID IN (56,57))
						AND NOT EXISTS (SELECT * FROM CONTACTS WHERE ACCTNO = @acctno AND CCODE = 'S03')
					BEGIN
						--Get and increment counter	
						SET @doc_no = 0
						SELECT @doc_no = COUNTER FROM COUNTERSTBL WHERE DOC_CATEGORY = 'CDN' AND DOC_TYPE = 'D'
						UPDATE COUNTERSTBL SET COUNTER = COUNTER + 1 WHERE DOC_CATEGORY = 'CDN' AND DOC_TYPE = 'D'

						--Insert contact records
						INSERT INTO CONTACTS (DOC_NO, ACCTNO, SUBC, CCODE, F_NAME, L_NAME, TITLE, TEL1, EMAIL, HOLD, COMPANYNO, ADDED_DTE, ADDED_USR)
						SELECT
							@doc_no + 1
							, @acctno
							, '1'
							, 'S03'
							, (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 57) --first name
							, (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 56) --last name
							, (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 55) --title
							, (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 54) --tel
							, (SELECT fieldValue FROM #TempFieldData WHERE fieldID = 53) --email
							, 'N'
							, 1
							, GETDATE()
							, 'AUTO'
					END


				--keep track if file was processed 
					set @processedFile = 'true'				
			END


			--remove clutter
			if @LotsOfText = '<ArrayOfDataObjectFull xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema"/>'
			BEGIN
			--keep track if file was processed 
					set @processedFile = 'true'
			end


			SET @date = GETDATE()
			SET @spotio_id = '' 
			SET @name = ''  
			SET @user = '' --dont have example of. I dont think we need as we only care about added user in custvend
			SET @address = '' 
			SET @acctno = '' 
			SET @email = '' -- have accts can have multiple, will pull from id "73" label "Contact #1 Email" check with Aaron if this will cause any issues on upload
			SET @website = '' 
			SET @tel = '' 


			--create table with spotioid
			CREATE TABLE #TempFieldData2 
			(
				spotioId  VARCHAR(100)
			)
			INSERT INTO #TempFieldData2 (spotioId)
			SELECT data.value('(id)[1]', ' VARCHAR(100)')
			FROM @xml.nodes('/ArrayOfDataObjectFull/DataObjectFull') AS TEMPTABLE(data)

			--start loop here
			--need loop statement if more than 1 account updated
			DECLARE @updateLoopVar int, @numUpdates int
			set @updateLoopVar = 0
			set @numUpdates = (SELECT count(spotioId) FROM #TempFieldData2)

			while @updateLoopVar < @numUpdates
			BEGIN
			

			--for use in linking data in table3
			set @spotio_id = (SELECT TOP 1 spotioId FROM #TempFieldData2)
			set @acctno = ''
			
			--website spotio fieldId
			declare @spotioWebVar int, @spotioTelVar int, @spotioEmailVar int, @spotioAcctType int, @contact1Title int, @contact1Email int, @contact1Phone int, @contact1fname int, @contact1lname int, @contact2Title int, @contact2Email int, @contact2Phone int, @contact2fname int, @contact2lname int, @contact3Title int, @contact3Email int, @contact3Phone int, @contact3fname int, @contact3lname int, @OSFocusVar int, @spotioUserNameVar int, @insideUserEmailVar int
			set @spotioWebVar = 20
			set @spotioTelVar = 74
			set @spotioEmailVar = 73
			set @spotioAcctType = 10
			set @OSFocusVar = 14
			set @spotioUserNameVar = 21

			set @contact1Title = 72
			set @contact1Email = 73
			set @contact1Phone = 74
			set @contact1fname = 70
			set @contact1lname = 71

			set @contact2Title = 59
			set @contact2Email = 60
			set @contact2Phone = 61
			set @contact2fname = 63
			set @contact2lname = 62

			set @contact3Title = 55
			set @contact3Email = 53
			set @contact3Phone = 54
			set @contact3fname = 57
			set @contact3lname = 56

			--spotiocontact123 variables
			declare @spotiocontact1Title varchar(100), @spotiocontact1Email varchar(100), @spotiocontact1Phone varchar(100), @spotiocontact1fname varchar(100), @spotiocontact1lname varchar(100), @spotiocontact2Title varchar(100), @spotiocontact2Email varchar(100), @spotiocontact2Phone varchar(100), @spotiocontact2fname varchar(100), @spotiocontact2lname varchar(100), @spotiocontact3Title varchar(100), @spotiocontact3Email varchar(100), @spotiocontact3Phone varchar(100), @spotiocontact3fname varchar(100), @spotiocontact3lname varchar(100)

			--find values associated with spotioId or return null
			CREATE TABLE #TempFieldData3
			(
				spotioId  VARCHAR(100),
				acctno VARCHAR(20),
				acctname VARCHAR(60),
				addressValue VARCHAR(1000),
				email VARCHAR(100),
				website VARCHAR(100),
				tel VARCHAR(100),
				accttype VARCHAR(100),
				stageid VARCHAR(20),
				osfocus VARCHAR(100),
				userName varchar(100),
				contact1Title varchar(100),
				contact1Email varchar(100),
				contact1Phone varchar(100),
				contact1fname varchar(100),
				contact1lname varchar(100),
				contact2Title varchar(100),
				contact2Email varchar(100),
				contact2Phone varchar(100),
				contact2fname varchar(100),
				contact2lname varchar(100),
				contact3Title varchar(100),
				contact3Email varchar(100),
				contact3Phone varchar(100),
				contact3fname varchar(100),
				contact3lname varchar(100),

			)
			INSERT INTO #TempFieldData3 (spotioId, acctno, acctname, addressValue, email, website, tel, accttype, stageid, osfocus, userName, contact1Title, contact1Email, contact1Phone, contact1fname, contact1lname, contact2Title, contact2Email, contact2Phone, contact2fname, contact2lname, contact3Title, contact3Email, contact3Phone, contact3fname, contact3lname)
			select data.value(N'(DataObjectFull/id)[1]', ' VARCHAR(100)')
			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/externalDataObjectId)[1]', N'varchar(20)') AS acctno
			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/name)[1]', N'varchar(60)') AS acctname
			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/pin/address)[1]', N'varchar(1000)') AS addressValue
			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/fields/Field[(fieldId)[1] = sql:variable("@spotioEmailVar")]/value)[1]', N'varchar(100)') AS email
			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/fields/Field[(fieldId)[1] = sql:variable("@spotioWebVar")]/value)[1]', N'varchar(100)') AS website
			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/fields/Field[(fieldId)[1] = sql:variable("@spotioTelVar")]/value)[1]', N'varchar(100)') AS tel
			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/fields/Field[(fieldId)[1] = sql:variable("@spotioAcctType")]/value)[1]', N'varchar(100)') AS accttype
			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/stageId)[1]', N'varchar(100)') AS stageid
			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/fields/Field[(fieldId)[1] = sql:variable("@OSFocusVar")]/value)[1]', N'varchar(100)') AS osfocus
			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/fields/Field[(fieldId)[1] = sql:variable("@spotioUserNameVar")]/value)[1]', N'varchar(100)') AS userName

			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/fields/Field[(fieldId)[1] = sql:variable("@contact1Title")]/value)[1]', N'varchar(100)') AS contact1Title
			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/fields/Field[(fieldId)[1] = sql:variable("@contact1Email")]/value)[1]', N'varchar(100)') AS contact1Email
			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/fields/Field[(fieldId)[1] = sql:variable("@contact1Phone")]/value)[1]', N'varchar(100)') AS contact1Phone
			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/fields/Field[(fieldId)[1] = sql:variable("@contact1fname")]/value)[1]', N'varchar(100)') AS contact1fname
			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/fields/Field[(fieldId)[1] = sql:variable("@contact1lname")]/value)[1]', N'varchar(100)') AS contact1lname
			
			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/fields/Field[(fieldId)[1] = sql:variable("@contact2Title")]/value)[1]', N'varchar(100)') AS contact2Title
			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/fields/Field[(fieldId)[1] = sql:variable("@contact2Email")]/value)[1]', N'varchar(100)') AS contact2Email
			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/fields/Field[(fieldId)[1] = sql:variable("@contact2Phone")]/value)[1]', N'varchar(100)') AS contact2Phone
			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/fields/Field[(fieldId)[1] = sql:variable("@contact2fname")]/value)[1]', N'varchar(100)') AS contact2fname
			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/fields/Field[(fieldId)[1] = sql:variable("@contact2lname")]/value)[1]', N'varchar(100)') AS contact2lname
			
			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/fields/Field[(fieldId)[1] = sql:variable("@contact3Title")]/value)[1]', N'varchar(100)') AS contact3Title
			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/fields/Field[(fieldId)[1] = sql:variable("@contact3Email")]/value)[1]', N'varchar(100)') AS contact3Email
			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/fields/Field[(fieldId)[1] = sql:variable("@contact3Phone")]/value)[1]', N'varchar(100)') AS contact3Phone
			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/fields/Field[(fieldId)[1] = sql:variable("@contact3fname")]/value)[1]', N'varchar(100)') AS contact3fname
			, data.value(N'(DataObjectFull[(id)[1] = sql:variable("@spotio_id")]/fields/Field[(fieldId)[1] = sql:variable("@contact3lname")]/value)[1]', N'varchar(100)') AS contact3lname
			FROM @xml.nodes('/ArrayOfDataObjectFull') AS TEMPTABLE(data)

			

			set @acctno = (SELECT TOP 1 acctno FROM #TempFieldData3)
			SET @name = RTRIM(REPLACE(ISNULL((SELECT TOP 1 acctname FROM #TempFieldData3),''), RTRIM(@acctno), ''))
			if @name is null
			begin
			SET @name = (SELECT TOP 1 acctname FROM #TempFieldData3)
			end
			set @address = (SELECT TOP 1 addressValue FROM #TempFieldData3)
			set @email = (SELECT TOP 1 email FROM #TempFieldData3)
			set @website = (SELECT TOP 1 website FROM #TempFieldData3)
			set @tel = (SELECT TOP 1 tel FROM #TempFieldData3)
			set @account_type = (SELECT TOP 1 CS.ACCOUNT_TYPE FROM CUSTVENDSETUP CS JOIN TBLCODE T ON CS.ACCOUNT_TYPE = T.TBLCODE AND TBLTYPE = '002' where name = (SELECT TOP 1 accttype FROM #TempFieldData3))
			set @stageid = (SELECT TOP 1 stageid FROM #TempFieldData3)
			set @OSFocus = (SELECT TOP 1 osfocus FROM #TempFieldData3)
			set @username = (SELECT TOP 1 userName FROM #TempFieldData3)


			set @spotiocontact1Title = (SELECT TOP 1 contact1Title FROM #TempFieldData3)
			set @spotiocontact1Email = (SELECT TOP 1 contact1Email FROM #TempFieldData3)
			set @spotiocontact1Phone = (SELECT TOP 1 contact1Phone FROM #TempFieldData3)
			set @spotiocontact1fname = (SELECT TOP 1 contact1fname FROM #TempFieldData3)
			set @spotiocontact1lname = (SELECT TOP 1 contact1lname FROM #TempFieldData3)
			
			set @spotiocontact2Title = (SELECT TOP 1 contact2Title FROM #TempFieldData3)
			set @spotiocontact2Email = (SELECT TOP 1 contact2Email FROM #TempFieldData3)
			set @spotiocontact2Phone = (SELECT TOP 1 contact2Phone FROM #TempFieldData3)
			set @spotiocontact2fname = (SELECT TOP 1 contact2fname FROM #TempFieldData3)
			set @spotiocontact2lname = (SELECT TOP 1 contact2lname FROM #TempFieldData3)
			
			set @spotiocontact3Title = (SELECT TOP 1 contact3Title FROM #TempFieldData3)
			set @spotiocontact3Email = (SELECT TOP 1 contact3Email FROM #TempFieldData3)
			set @spotiocontact3Phone = (SELECT TOP 1 contact3Phone FROM #TempFieldData3)
			set @spotiocontact3fname = (SELECT TOP 1 contact3fname FROM #TempFieldData3)
			set @spotiocontact3lname = (SELECT TOP 1 contact3lname FROM #TempFieldData3)

			--find user from gathered username (only for input new account)
			set @insideUser = (SELECT MIN(U.CCODE) FROM USERLIST U where U.F_NAME = LEFT(@username, CHARINDEX(' ', @username) - 1) AND U.L_NAME = RIGHT(RTRIM(@username), LEN(RTRIM(@username)) - CHARINDEX(' ', @username)))
			

			--if no assigned value, look up in spotio to hadco uid linking table
			if (@acctno = '') or (@acctno is null)
			BEGIN
				SELECT @acctno = acctno
				FROM Hadco_SpotIO_ID
				WHERE spotio_id = @spotio_id
			End
			
			--Parse address
			DECLARE @addr1_2 varchar(50), @city_2 varchar(30), @state_2 varchar(4), @zip_2 varchar(12)
			DECLARE @comma1_2 int, @comma2_2 int, @comma3_2 int, @substr2_2 varchar(1000), @substr3_2 varchar(1000)
			SELECT @comma1_2 = CHARINDEX(',', @address)
			SELECT @substr2_2 = RIGHT(@address, LEN(@address) - @comma1_2)
			SELECT @comma2_2 = CHARINDEX(',', @substr2_2)
			SELECT @substr3_2 = RIGHT(@substr2_2, LEN(@substr2_2) - @comma2_2)
			SELECT @comma3_2 = CHARINDEX(',', @substr3_2)

			SELECT @addr1_2 = LEFT(@address, @comma1_2 - 1)
			SELECT @city_2 = LTRIM(LEFT(@substr2_2, @comma2_2 - 1))
			SELECT @state_2 = LEFT(LTRIM(LEFT(@substr3_2, @comma3_2 - 1)),2)
			SELECT @zip_2 = REPLACE(LTRIM(LEFT(@substr3_2, @comma3_2 - 1)), @state_2 + ' ', '')


			
			-- check if acct is approved customer
			set @approved = ''
			set @approved = (select APPROVED from CUSTVENDSETUP where @acctno = ACCTNO)

			/*
			print @acctno
			print @spotio_id
			print @name
			print @user
			print @addr1_2
			print @city_2
			print @state_2
			print @zip_2
			print @tel
			print @email
			print @stageid
			print @osfocus
			print @website
			print @account_type
			print @insideuser
			*/

			--create new customer if necessary
			if @acctno is null
			begin
			--add acct
			If @name <> '' AND @spotio_id NOT IN (SELECT spotio_id FROM Hadco_SpotIO_ID)
				BEGIN
					--for these files, there is no outside sales person, so we assign AUTO
					SET @user = 'AUTO'
					--Much of this section borrowed from uspAddNewCustumer, from the old Call Report app
					select @next=cust_count from compsetup1
					set @next=@next+1
					update compsetup1 set cust_count=cust_count+1
					SET @acctno = CAST(@next AS varchar(12))

					--Insert customer
					insert into "CUSTVEND"
						("ACCTNO", "SUBC", "CUST_VEND","REGION","NAME","ADR1","ADR2","CITY","STATE","ZIP","COUNTRY","WEB_SITE","TEL1","EMAIL","FAX_TIME", "ALT1_CODE_C", "ALT2_CODE_C", "ALT3_CODE_C", "ALT4_CODE_C", "PRINT_TARGET", "PRINT_EMAIL_FORMAT", "INTERCOMPANY_ACCOUNT", "ADDED_USR", "ADDED_DTE")
					VALUES
					(                             
						@acctno
						, '1'
						, 'C'
						, @user
						, @name
						, @addr1_2                                                                                                                                             --addr1 = everything before the first comma
						, ''
						, @city_2
						, @state_2
						, @zip_2
						, 'US'
						, @website
						, @tel
						, @email
						,'00:00'
						, NULL
						, NULL
						, NULL
						, ''
						, '2'
						, '1'
						, 'N'
						, @user
						, getdate()
					)

					insert into "CUSTVENDSETUP"
						("ACCTNO", "SUBC", "CUST_VEND", "PRIORITY", "ACCOUNT_TYPE", "RCV_OVER_SHIP", "SHIP_DEF_NO", "BILL_DEF_NO", "PAY_DEF_NO", "PAY_US", "COMPANYNO", "DIVISION", "DEPART", "FORM_1099", "HOLD", "HOLD_DATE", "HOLD_BY", "APPROVED", "APPROVED_DATE", "APPROVED_BY", "ULINES","TERM_CODE" ,"SHIP_COMPLETE", "POST_SHIP_DOC", "INV_TYPE", "ACCOUNT_RATE", "EARLY_SHIPMENT", "DAYS_BEFORE_SHIPPING", "FOB", "FOB_LBL", "GL_ACCOUNT", "CURENCY_CONV", "MIN_ORDER_LINE", "MIN_ORDER", "QT_TYPE_DEF", "SO_TYPE_DEF", "RF_TYPE_DEF", "PO_TYPE_DEF", "SMAN1_NG", "SMAN2_NG", "SMAN3_NG", "SMAN4_NG", "SMAN5_NG", "SMAN1_DOCLN", "SMAN2_DOCLN", "SMAN3_DOCLN", "SMAN4_DOCLN", "SMAN5_DOCLN", "DISC_TYPE", "DISC_DOCLN", "MISC1_DOCLN", "MISC2_DOCLN", "MISC3_DOCLN", "MISC4_DOCLN", "MISC5_DOCLN", "MISC6_DOCLN", "MISC1_TYPE", "MISC2_TYPE", "MISC3_TYPE", "MISC4_TYPE", "MISC5_TYPE", "MISC6_TYPE", "MISC1_PRN", "MISC2_PRN", "MISC3_PRN", "MISC4_PRN", "MISC5_PRN", "MISC6_PRN", "MISC1_TTL", "MISC2_TTL", "MISC3_TTL", "MISC4_TTL", "MISC5_TTL", "MISC6_TTL", "SUBT_TAX_A", "SUBT_TAX_B", "SUBT_TAX_C", "M1_TAX_A", "M1_TAX_B", "M1_TAX_C", "M2_TAX_A", "M2_TAX_B", "M2_TAX_C", "M3_TAX_A", "M3_TAX_B", "M3_TAX_C", "M4_TAX_A", "M4_TAX_B", "M4_TAX_C", "M5_TAX_A", "M5_TAX_B", "M5_TAX_C", "M6_TAX_A", "M6_TAX_B", "M6_TAX_C", "TAX_A_CODE", "TAX_B_CODE", "TAX_C_CODE", "TAX_A_TTL", "TAX_B_TTL", "TAX_C_TTL", "EXCH_CORE_CHRG", "EXCH_CORE_RETURN", "EXCH_CORE_NOTE", "EXCH_CHARGE_FROM", "EXCH_CHARGE_COST", "EXCH_CORE_COST", "EXCH_CORE_PERC", "SMAN1_CODE", "ADDED_USR", "ADDED_DTE")
					VALUES
					(
						@acctno
						, '1'
						, 'C'
						, (CASE WHEN @osfocus = 'Y' THEN 'P' --High Priority Customer
								WHEN @stageid = '5' THEN 'X' --Not a Good Fit
								WHEN @stageid = '6' THEN 'Z' --Business Closed
								ELSE NULL
							END) 
						, @account_type --Account Type
						, 1.500000000000000e+001
						, 0
						, 0
						, 0
						, '02L'
						, 1
						, NULL
						, NULL
						, 'N'
						, 'Y'
						, getdate()
						, @user
						, 'N'
						, getdate()
						, @user
						, 'N','RTBD', 'N', 'Y', 'D', NULL, 'N', 0, 'DST', 'FOB', '40000', 'USD', 0.000000000000000e+000, 4.000000000000000e+001, 'Q', 'S', 'R', 'P', 'N', 'N', 'N', 'N', 'N', 'D', 'D', 'D', 'D', 'D'
						, '0', 'D', 'D', 'D', 'D', 'D', 'D', 'D', '1', '1', '1', '1', '1', '1', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N', 'Y', 'N', 'N', 'Y', 'N', 'N', 'Y', 'N'
						, 'N', 'Y', 'N', 'N', 'Y', 'N', 'N', NULL, NULL, NULL, 'N', 'N', 'N', 'N', 30, 'Y', '2', '1', '5', 10
						, @insideUser
						, @user
						, getdate()
					)

					--Record Pentagon acctno / SpotIO ID relationship
					INSERT INTO Hadco_SpotIO_ID (acctno, spotio_id)
					VALUES (@acctno, @spotio_id)

					--keep track if file was processed
					set @processedFile = 'true'
				END	

			--end adding account
			END

			--if we are able to pull an acctno to run off, we should be able to pull the info and modify not approved customer
			If @acctno <> ''
			begin
				--not changing atm "REGION" (not available), "ADDED_USR" (not available), "ADDED_DTE" (we should never have to change)
				UPDATE "CUSTVEND"
					set
					"WEB_SITE" = @website
					, "ALT3_CODE_C" = @OSFocus
					where "ACCTNO" = @acctno

					UPDATE "CUSTVENDSETUP"
					set
					"ACCOUNT_TYPE" = @account_type
					where "acctno" = @acctno


					
					--Contact 1
					--Update
					IF (@spotiocontact1fname <> '' and @spotiocontact1lname <> ''
						AND EXISTS (SELECT * FROM CONTACTS WHERE ACCTNO = @acctno AND CCODE = 'S01'))
					BEGIN
						UPDATE CONTACTS
						SET F_NAME = (@spotiocontact1fname) --first name
							, L_NAME = (@spotiocontact1lname) --last name
							, TITLE = (@spotiocontact1Title) --title
							, TEL1 = (@spotiocontact1Phone) --tel
							, EMAIL = (@spotiocontact1Email) --email
							, UPDATED_USR = 'AUTO'
							, UPDATED_DTE = GETDATE()
						WHERE ACCTNO = @acctno
						AND CCODE = 'S01'
					END
					--Insert
					IF (@spotiocontact1fname <> '' and @spotiocontact1lname <> ''
						AND NOT EXISTS (SELECT * FROM CONTACTS WHERE ACCTNO = @acctno AND CCODE = 'S01'))
					BEGIN
						--Get and increment counter	
						SELECT @doc_no = COUNTER FROM COUNTERSTBL WHERE DOC_CATEGORY = 'CDN' AND DOC_TYPE = 'D'
						UPDATE COUNTERSTBL SET COUNTER = COUNTER + 1 WHERE DOC_CATEGORY = 'CDN' AND DOC_TYPE = 'D'

						--Insert contact records
						INSERT INTO CONTACTS (DOC_NO, ACCTNO, SUBC, CCODE, F_NAME, L_NAME, TITLE, TEL1, EMAIL, HOLD, COMPANYNO, ADDED_DTE, ADDED_USR)
						SELECT
							@doc_no + 1
							, @acctno
							, '1'
							, 'S01'
							, (@spotiocontact1fname) --first name
							, (@spotiocontact1lname) --last name
							, (@spotiocontact1Title) --title
							, (@spotiocontact1Phone) --tel
							, (@spotiocontact1Email) --email
							, 'N'
							, 1
							, GETDATE()
							, 'AUTO'

						--Update default contact, if none exists
						UPDATE CUSTVENDSETUP
						SET CCODE = 'S01'
						WHERE ACCTNO = @acctno
						AND CUST_VEND = 'C'
						AND ISNULL(CCODE,'') = ''
					END

					--Contact 2
					--Update
					IF (@spotiocontact2fname <> '' and @spotiocontact2lname <> ''
						AND EXISTS (SELECT * FROM CONTACTS WHERE ACCTNO = @acctno AND CCODE = 'S02'))
					BEGIN
						UPDATE CONTACTS
						SET F_NAME = (@spotiocontact2fname) --first name
							, L_NAME = (@spotiocontact2lname) --last name
							, TITLE = (@spotiocontact2Title) --title
							, TEL1 = (@spotiocontact2Phone) --tel
							, EMAIL = (@spotiocontact2Email) --email
							, UPDATED_USR = 'AUTO'
							, UPDATED_DTE = GETDATE()
						WHERE ACCTNO = @acctno
						AND CCODE = 'S02'
					END
					--Insert
					IF (@spotiocontact2fname <> '' and @spotiocontact2lname <> ''
						AND NOT EXISTS (SELECT * FROM CONTACTS WHERE ACCTNO = @acctno AND CCODE = 'S02'))
					BEGIN
						--Get and increment counter	
						SELECT @doc_no = COUNTER FROM COUNTERSTBL WHERE DOC_CATEGORY = 'CDN' AND DOC_TYPE = 'D'
						UPDATE COUNTERSTBL SET COUNTER = COUNTER + 1 WHERE DOC_CATEGORY = 'CDN' AND DOC_TYPE = 'D'

						--Insert contact records
						INSERT INTO CONTACTS (DOC_NO, ACCTNO, SUBC, CCODE, F_NAME, L_NAME, TITLE, TEL1, EMAIL, HOLD, COMPANYNO, ADDED_DTE, ADDED_USR)
						SELECT
							@doc_no + 1
							, @acctno
							, '1'
							, 'S02'
							, (@spotiocontact2fname) --first name
							, (@spotiocontact2lname) --last name
							, (@spotiocontact2Title) --title
							, (@spotiocontact2Phone) --tel
							, (@spotiocontact2Email) --email
							, 'N'
							, 1
							, GETDATE()
							, 'AUTO'

						--Update default contact, if none exists
						UPDATE CUSTVENDSETUP
						SET CCODE = 'S02'
						WHERE ACCTNO = @acctno
						AND CUST_VEND = 'C'
						AND ISNULL(CCODE,'') = ''
					END

					--Contact 3
					--Update
					IF (@spotiocontact3fname <> '' and @spotiocontact3lname <> ''
						AND EXISTS (SELECT * FROM CONTACTS WHERE ACCTNO = @acctno AND CCODE = 'S03'))
					BEGIN
						UPDATE CONTACTS
						SET F_NAME = (@spotiocontact3fname) --first name
							, L_NAME = (@spotiocontact3lname) --last name
							, TITLE = (@spotiocontact3Title) --title
							, TEL1 = (@spotiocontact3Phone) --tel
							, EMAIL = (@spotiocontact3Email) --email
							, UPDATED_USR = 'AUTO'
							, UPDATED_DTE = GETDATE()
						WHERE ACCTNO = @acctno
						AND CCODE = 'S03'
					END
					--Insert
					IF (@spotiocontact3fname <> '' and @spotiocontact3lname <> ''
						AND NOT EXISTS (SELECT * FROM CONTACTS WHERE ACCTNO = @acctno AND CCODE = 'S03'))
					BEGIN
						--Get and increment counter	
						SELECT @doc_no = COUNTER FROM COUNTERSTBL WHERE DOC_CATEGORY = 'CDN' AND DOC_TYPE = 'D'
						UPDATE COUNTERSTBL SET COUNTER = COUNTER + 1 WHERE DOC_CATEGORY = 'CDN' AND DOC_TYPE = 'D'

						--Insert contact records
						INSERT INTO CONTACTS (DOC_NO, ACCTNO, SUBC, CCODE, F_NAME, L_NAME, TITLE, TEL1, EMAIL, HOLD, COMPANYNO, ADDED_DTE, ADDED_USR)
						SELECT
							@doc_no + 1
							, @acctno
							, '1'
							, 'S03'
							, (@spotiocontact3fname) --first name
							, (@spotiocontact3lname) --last name
							, (@spotiocontact3Title) --title
							, (@spotiocontact3Phone) --tel
							, (@spotiocontact3Email) --email
							, 'N'
							, 1
							, GETDATE()
							, 'AUTO'

						--Update default contact, if none exists
						UPDATE CUSTVENDSETUP
						SET CCODE = 'S03'
						WHERE ACCTNO = @acctno
						AND CUST_VEND = 'C'
						AND ISNULL(CCODE,'') = ''
					END

					--keep track if file was processed
					set @processedFile = 'true'
				
			end

			--if acctno has value and not an approved cust

			If @acctno <> '' AND @approved = 'N'
			begin
				
				
				UPDATE "CUSTVEND"
					set
					"ADR1" = RTRIM(@addr1_2)
					,"CITY" = RTRIM(@city_2)
					,"STATE" = RTRIM(@state_2)
					,"ZIP" = RTRIM(@zip_2)
					,"NAME" = @name
					where "ACCTNO" = @acctno

					
					--Not a Good Fit
					if @stageid = '5'
					BEGIN
						UPDATE CUSTVENDSETUP
						SET PRIORITY = 'X'
						WHERE ACCTNO = @acctno
						AND CUST_VEND = 'C'
					END
					
					--Business Closed
					if @stageid = '6'
					BEGIN
						UPDATE CUSTVENDSETUP
						SET PRIORITY = 'Z'
						WHERE ACCTNO = @acctno
						AND CUST_VEND = 'C'
					END

					--keep track if file was processed
					set @processedFile = 'true'
				
			end	

			--end adding account
			
			end
			--end loop here
			DELETE FROM #TempFieldData2 WHERE spotioId = @spotio_id
			set @updateLoopVar = @updateLoopVar + 1
			DROP TABLE #TempFieldData3
			END
			--if we have set the conditions for one of the previous steps we move the file to archive (create account, new call log data, or modify the account details)

			if @processedFile = 'true'
			BEGIN
				--Copy file to Archive
				SET @cmdquery = 'copy "D:\CallReportFiles\spotio\' + @filename + '"'
					+ ' "\\hpa-nas01\archive\CallReportApp_Files\"'
				EXEC master..xp_cmdshell @cmdquery
		
				--Delete file
				SET @cmdquery = 'del "D:\CallReportFiles\spotio\' + @filename +'"'
				EXEC master..xp_cmdshell @cmdquery
			END
			
			--nate stop messing
			
			SET @counter = @counter + 1
			
		END

		DROP TABLE #TempFieldData
		DROP TABLE #TempFieldData2
	END

END

END
