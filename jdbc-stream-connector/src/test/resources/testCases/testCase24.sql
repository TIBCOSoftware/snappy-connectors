INSERT INTO [testdatabase].[dbo].[ADJUSTMENT1] VALUES (912, 950000100105, 9933, 102, 506161, 775324, 431703, 28440200.7189,N'iT', N'2016-05-09', 520989, 966723, 592283, 682854, 165363, N'2016-07-02', N'2016-05-30', N'889', N'89');
INSERT INTO [testdatabase].[dbo].[ADJUSTMENT1] VALUES (912, 950000100106, 9933, 102, 506161, 775324, 431703, 28440200.7189,N'iT', N'2016-05-09', 520989, 966723, 592283, 682854, 165363, N'2016-07-02', N'2016-05-30', N'889', N'89');
update [testdatabase].[dbo].[ADJUSTMENT1] set DESCR = N'iTTTz' where adj_id = 950000100105;
update [testdatabase].[dbo].[ADJUSTMENT1] set DESCR = N'iTTTz' where adj_id = 950000100106;
delete from [testdatabase].[dbo].[ADJUSTMENT1] where adj_id = 950000100105;
INSERT INTO [testdatabase].[dbo].[ADJUSTMENT1] VALUES (10000, 999999999999924, 9000, 900, 506161, 775324, 431703, 28440200.7189,N'iTTTT', N'2016-05-09', 520989, 966723, 592283, 682854, 165363, N'2016-07-02', N'2016-05-30', N'889', N'89');
