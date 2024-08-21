USE [GDB_01_001_test]
GO
/****** Object:  StoredProcedure [dbo].[Hadco_Spotio_CallUpload]    Script Date: 8/21/2024 10:14:53 AM ******/
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
			PRINT @filename
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


			DECLARE @spotio_id varchar(100), @account_type varchar(5), @name varchar(60), @user varchar(10), @date datetime, @address varchar(1000), @acctno varchar(20), @stageid varchar(20)
			
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

				--Get date
				SELECT @date = data.value('(date)[1]','DATETIME')
				FROM @xml.nodes('/ActivityOutput/ActivityItem') AS TEMPTABLE(data)

				--Get Spotio ID and name
				SELECT @spotio_id = ISNULL(data.value('(id)[1]','VARCHAR(100)'),'')
				--acctno has no value
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

				--If new customer, insert into Pentagon
				If @name <> '' AND @spotio_id NOT IN (SELECT spotio_id FROM Hadco_SpotIO_ID)
				BEGIN
					--Get details about the new customer
					DECLARE @tel varchar(100)
					SELECT @tel = data.value('(string)[1]', 'VARCHAR(100)')
					FROM @xml.nodes('/ActivityOutput/CustomerData/phones') AS TEMPTABLE(data)

					DECLARE @email varchar(100)
					SELECT @email = data.value('(string)[1]', 'VARCHAR(100)')
					FROM @xml.nodes('/ActivityOutput/CustomerData/emails') AS TEMPTABLE(data)

					--Parse address
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

				END	

				
				IF @name <> '' AND @spotio_id IN (SELECT spotio_id FROM Hadco_SpotIO_ID)
				BEGIN
					----------------------------------------------------------
					--Update Priority
					----------------------------------------------------------
					SELECT @acctno = acctno
					FROM Hadco_SpotIO_ID
					WHERE spotio_id = @spotio_id

					--Not a Good Fit
					UPDATE CUSTVENDSETUP
					SET PRIORITY = 'X'
					WHERE ACCTNO = @acctno
					AND CUST_VEND = 'C'
					AND @stageid = '5'
					
					--Business Closed
					UPDATE CUSTVENDSETUP
					SET PRIORITY = 'Z'
					WHERE ACCTNO = @acctno
					AND CUST_VEND = 'C'
					AND @stageid = '6'

					----------------------------------------------------------
					--Update call tracking
					----------------------------------------------------------
					DECLARE @DocNo as nvarchar(20)

					--increment by 1 and get the doc no 
					UPDATE [GDB_01_001_test].[dbo].[COUNTERSTBL]
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

				--Copy file to Archive
				SET @cmdquery = 'copy "D:\CallReportFiles\spotio\' + @filename + '"'
					+ ' "\\hpa-nas01\archive\CallReportApp_Files\"'
				EXEC master..xp_cmdshell @cmdquery
		
				--Delete file
				SET @cmdquery = 'del "D:\CallReportFiles\spotio\' + @filename +'"'
				EXEC master..xp_cmdshell @cmdquery

			END

			SET @counter = @counter + 1
			
		END

		DROP TABLE #TempFieldData
	END

END

END
