-----קשרי ילד לאבא
INSERT INTO Relatives (Person_Id, Relative_Id, Connection_Type)
SELECT Person_Id, Father_Id, N'אב'
FROM Persons
WHERE Father_Id IS NOT NULL;


---קשרי ילד לאמא
INSERT INTO Relatives (Person_Id, Relative_Id, Connection_Type)
SELECT Person_Id, Mother_Id, N'אם'
FROM Persons
WHERE Mother_Id IS NOT NULL;



----קשרי אבא וילד
INSERT INTO Relatives (Person_Id, Relative_Id, Connection_Type)
SELECT Father_Id, Person_Id,
       CASE WHEN Gender = N'זכר' THEN N'בן' ELSE N'בת' END
FROM Persons
WHERE Father_Id IS NOT NULL;

----קשרי אמא וילד
INSERT INTO Relatives (Person_Id, Relative_Id, Connection_Type)
SELECT Mother_Id, Person_Id,
       CASE WHEN Gender = N'זכר' THEN N'בן' ELSE N'בת' END
FROM Persons
WHERE Mother_Id IS NOT NULL;


---קשרי אח\אחות
INSERT INTO Relatives (Person_Id, Relative_Id, Connection_Type)
SELECT p1.Person_Id, p2.Person_Id,
       CASE WHEN p2.Gender = N'זכר' THEN N'אח' ELSE N'אחות' END
FROM Persons p1
JOIN Persons p2 ON
    p1.Person_Id <> p2.Person_Id AND
    p1.Father_Id = p2.Father_Id AND
    p1.Mother_Id = p2.Mother_Id
WHERE p1.Father_Id IS NOT NULL AND p1.Mother_Id IS NOT NULL;



----קשרי בן \בת זוג
INSERT INTO Relatives (Person_Id, Relative_Id, Connection_Type)
SELECT Person_Id, Spouse_Ids,
       CASE WHEN Gender = N'זכר' THEN N'בת זוג' ELSE N'בן זוג' END
FROM Persons
WHERE Spouse_Ids IS NOT NULL;

----השלמת בני זוג חסרים לפני הצד השני
UPDATE P2
SET P2.Spouse_Ids = P1.Person_Id
FROM Persons P1
JOIN Persons P2 ON P1.Spouse_Ids = P2.Person_Id
WHERE P2.Spouse_Ids IS NULL;

