package pubmed;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import cores.avro.BatchAvroColumnWriter;

public class xmlTran {
    static Schema sPubmedArticle;
    static Schema sMedlineCitation;
    static Schema sMCPMID;
    static Schema sArticle;
    static Schema sJournal;
    static Schema sISSN;
    static Schema sJournalIssue;
    static Schema sPubDate;
    static Schema sELocationID;
    static Schema sAbstractText;
    static Schema sAuthor;
    static Schema sDataBank;
    static Schema sGrant;
    static Schema sPublicationType;
    static Schema sArticleDate;
    static Schema sMedlineJournalInfo;
    static Schema sChemical;
    static Schema sNameOfSubstance;
    static Schema sSupplMeshName;
    static Schema sCommentsCorrections;
    static Schema sCCPMID;
    static Schema sMeshHeading;
    static Schema sDescriptorName;
    static Schema sQualifierName;
    static Schema sOtherID;
    static Schema sOtherAbstract;
    static Schema sKeyword;
    static Schema sInvestigator;
    static Schema sGeneralNote;
    static Schema sPersonalNameSubject;
    static Schema sPubmedData;
    static Schema sPubmedPubDate;
    static Schema sArticleId;

    static void tranPubmedArticleSet(File[] xmlFiles, String schemaPath, String resultPath, int free, int mul) {
        SAXReader reader = new SAXReader();
        try {
            Schema s = new Schema.Parser().parse(new File(schemaPath + File.separatorChar + "PubmedArticle.avsc"));
            BatchAvroColumnWriter<Record> writer = new BatchAvroColumnWriter<Record>(s, resultPath, free, mul);
            int count = 0;
            for (File f : xmlFiles) {
                System.gc();
                Document document = reader.read(f);
                Element root = document.getRootElement();
                Iterator it = root.elementIterator("PubmedArticle");
                //            DatumWriter<Record> writer = new GenericDatumWriter<Record>(s);
                //            DataFileWriter<Record> fileWriter = new DataFileWriter<Record>(writer);
                //            fileWriter.create(s, new File(avroPath));
                while (it.hasNext()) {
                    Element article = (Element) it.next();
                    Record v = tranPubmedArticle(article, schemaPath);
                    writer.append(v);
                    count++;
                    //                fileWriter.append(v);
                }
                it = null;
                root.clearContent();
                document.clearContent();
                //            fileWriter.close();
            }
            int index = writer.flush();
            File[] files = new File[index];
            for (int i = 0; i < index; i++)
                files[i] = new File(resultPath + "file" + String.valueOf(i) + ".trv");
            if (index == 1) {
                new File(resultPath + "file0.head").renameTo(new File(resultPath + "result.head"));
                new File(resultPath + "file0.trv").renameTo(new File(resultPath + "result.trv"));
            } else {
                writer.mergeFiles(files);
            }
            System.out.println("count: " + count);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (DocumentException e) {
            e.printStackTrace();
        }
    }

    static Record tranPubmedArticle(Element ele, String schemaPath) throws IOException {
        if (sPubmedArticle == null)
            sPubmedArticle = new Schema.Parser()
                    .parse(new File(schemaPath + File.separatorChar + "PubmedArticle.avsc"));
        Record value = new Record(sPubmedArticle);
        value.put("MedlineCitation", tranMedlineCitation(ele.element("MedlineCitation"),
                schemaPath + File.separatorChar + "MedlineCitation"));
        value.put("PubmedData",
                tranPubmedData(ele.element("PubmedData"), schemaPath + File.separatorChar + "PubmedData"));
        return value;
    }

    static Record tranMedlineCitation(Element ele, String schemaPath) throws IOException {
        if (sMedlineCitation == null)
            sMedlineCitation = new Schema.Parser()
                    .parse(new File(schemaPath + File.separatorChar + "MedlineCitation.avsc"));
        Record value = new Record(sMedlineCitation);
        value.put("Status", seri(ele.attributeValue("Status")));
        value.put("Owner", seri(ele.attributeValue("Owner")));
        value.put("MCPMID", tranMCPMID(ele.element("PMID"), schemaPath + File.separatorChar + "MCPMID"));
        value.put("DateCreated", tranDate(ele.element("DateCreated")));
        value.put("DateCompleted", tranDate(ele.element("DateCompleted")));
        value.put("DateRevised", tranDate(ele.element("DateRevised")));
        value.put("Article", tranArticle(ele.element("Article"), schemaPath + File.separatorChar + "Article"));
        value.put("MedlineJournalInfo", tranMedlineJournalInfo(ele.element("MedlineJournalInfo"),
                schemaPath + File.separatorChar + "MedlineJournalInfo"));
        value.put("ChemicalList",
                tranChemicalList(ele.element("ChemicalList"), schemaPath + File.separatorChar + "ChemicalList"));
        value.put("SuppMeshList",
                tranSupplMeshList(ele.element("SuppMeshList"), schemaPath + File.separatorChar + "SuppMeshList"));
        value.put("CitationSubset", seri(ele.elementText("CitationSubset")));
        value.put("GeneSymbolList",
                tranGeneSymbolList(ele.element("GeneSymbolList"), schemaPath + File.separatorChar + "GeneSymbolList"));
        value.put("CommentsCorrectionsList", tranCommentsCorrectionsList(ele.element("CommentsCorrectionsList"),
                schemaPath + File.separatorChar + "CommentsCorrectionsList"));
        value.put("MeshHeadingList", tranMeshHeadingList(ele.element("MeshHeadingList"),
                schemaPath + File.separatorChar + "MeshHeadingList"));
        value.put("NumberOfReferences", seri(ele.elementText("NumberOfReferences")));

        Iterator it = ele.elementIterator("OtherID");
        List<Record> oid = new ArrayList<Record>();
        while (it.hasNext()) {
            oid.add(tranOtherID((Element) it.next(),
                    schemaPath + File.separatorChar + "OtherIDList" + File.separatorChar + "OtherID"));
        }
        value.put("OtherIDList", oid);
        it = null;

        value.put("OtherAbstract",
                tranOtherAbstract(ele.element("OtherAbstract"), schemaPath + File.separatorChar + "OtherAbstract"));

        Element e = ele.element("KeywordList");
        if (e != null) {
            value.put("KLOwner", seri(e.attributeValue("Owner")));
        } else {
            value.put("KLOwner", seri(null));
        }
        value.put("KeywordList", tranKeywordList(e, schemaPath + File.separatorChar + "KeywordList"));

        it = ele.elementIterator("SpaceFlightMission");
        List<String> sfm = new ArrayList<String>();
        while (it.hasNext()) {
            sfm.add(((Element) it.next()).getStringValue());
        }
        value.put("SpaceFlightMissionList", sfm);
        it = null;

        value.put("InvestigatorList", tranInvestigatorList(ele.element("InvestigatorList"),
                schemaPath + File.separatorChar + "InvestigatorList"));
        value.put("GeneralNote",
                tranGeneralNote(ele.element("GeneralNote"), schemaPath + File.separatorChar + "GeneralNote"));
        value.put("PersonalNameSubjectList", tranPersonalNameSubjectList(ele.element("PersonalNameSubjectList"),
                schemaPath + File.separatorChar + "PersonalNameSubjectList"));
        return value;
    }

    static String tranDate(Element ele) {
        if (ele == null)
            return "*";
        else
            return ele.elementText("Year") + "-"
                    + String.format("%02d", Integer.parseInt(ele.elementText("Month").trim())) + "-"
                    + String.format("%02d", Integer.parseInt(ele.elementText("Day").trim()));
    }

    static Record tranMCPMID(Element ele, String schemaPath) throws IOException {
        if (sMCPMID == null)
            sMCPMID = new Schema.Parser().parse(new File(schemaPath + File.separatorChar + "MCPMID.avsc"));
        Record value = new Record(sMCPMID);
        value.put("MCVersion", seri(ele.attributeValue("Version")));
        value.put("MCpmid", Integer.parseInt(ele.getStringValue().trim()));
        return value;
    }

    static Record tranArticle(Element ele, String schemaPath) throws IOException {
        if (sArticle == null)
            sArticle = new Schema.Parser().parse(new File(schemaPath + File.separatorChar + "Article.avsc"));
        Record value = new Record(sArticle);
        value.put("PubModel", seri(ele.attributeValue("PubModel")));
        value.put("Journal", tranJournal(ele.element("Journal"), schemaPath + File.separatorChar + "Journal"));
        value.put("ArticleTitle", seri(ele.elementText("ArticleTitle")));
        Element e = ele.element("Pagination");
        if (e != null)
            value.put("Pagination", e.elementText("MedlinePgn"));
        else
            value.put("Pagination", seri(null));
        List<Record> v = new ArrayList<Record>();
        Iterator it = ele.elementIterator("ELocationID");
        while (it.hasNext()) {
            v.add(tranELocationID((Element) it.next(),
                    schemaPath + File.separatorChar + "ELocationIDList" + File.separatorChar + "ELocationID"));
        }
        value.put("ELocationIDList", v);

        e = ele.element("Abstract");
        if (e != null) {
            value.put("CopyrightInformation", seri(e.attributeValue("CopyrightInformation")));
        } else {
            value.put("CopyrightInformation", seri(null));
        }
        value.put("Abstract", tranAbstract(e, schemaPath + File.separatorChar + "Abstract"));

        e = ele.element("AuthorList");
        if (e != null) {
            value.put("AuCompleteYN", e.attributeValue("CompleteYN"));
        } else {
            value.put("AuCompleteYN", seri(null));
        }
        value.put("AuthorList", tranAuthorList(e, schemaPath + File.separatorChar + "AuthorList"));

        value.put("Language", seri(ele.elementText("Language")));

        e = ele.element("DataBankList");
        if (e != null) {
            value.put("DBCompleteYN", seri(e.attributeValue("CompleteYN")));
        } else {
            value.put("DBCompleteYN", seri(null));
        }
        value.put("DataBankList", tranDataBankList(e, schemaPath + File.separatorChar + "DataBankList"));

        e = ele.element("GrantList");
        if (e != null) {
            value.put("GrCompleteYN", seri(e.attributeValue("CompleteYN")));
        } else {
            value.put("GrCompleteYN", seri(null));
        }
        value.put("GrantList", tranGrantList(e, schemaPath + File.separatorChar + "GrantList"));

        value.put("PublicationTypeList", tranPublicationTypeList(ele.element("PublicationTypeList"),
                schemaPath + File.separatorChar + "PublicationTypeList"));
        value.put("ArticleDate",
                tranArticleDate(ele.element("ArticleDate"), schemaPath + File.separatorChar + "ArticleDate"));
        value.put("VernacularTitle", seri(ele.elementText("VernacularTitle")));
        return value;
    }

    static Record tranJournal(Element ele, String schemaPath) throws IOException {
        if (sJournal == null)
            sJournal = new Schema.Parser().parse(new File(schemaPath + File.separatorChar + "Journal.avsc"));
        Record value = new Record(sJournal);
        value.put("ISSN", tranISSN(ele.element("ISSN"), schemaPath + File.separatorChar + "ISSN"));
        value.put("JournalIssue",
                tranJournalIssue(ele.element("JournalIssue"), schemaPath + File.separatorChar + "JournalIssue"));
        value.put("Title", seri(ele.elementText("Title")));
        value.put("ISOAbbreviation", seri(ele.elementText("ISOAbbreviation")));
        return value;
    }

    static Record tranISSN(Element ele, String schemaPath) throws IOException {
        if (sISSN == null)
            sISSN = new Schema.Parser().parse(new File(schemaPath + File.separatorChar + "ISSN.avsc"));
        Record value = new Record(sISSN);
        if (ele != null) {
            value.put("IssnType", seri(ele.attributeValue("IssnType")));
            value.put("issn", seri(ele.getStringValue()));
        } else {
            value.put("IssnType", seri(null));
            value.put("issn", seri(null));
        }
        return value;
    }

    static Record tranJournalIssue(Element ele, String schemaPath) throws IOException {
        if (sJournalIssue == null)
            sJournalIssue = new Schema.Parser().parse(new File(schemaPath + File.separatorChar + "JournalIssue.avsc"));
        Record value = new Record(sJournalIssue);
        value.put("CitedMedium", seri(ele.attributeValue("CitedMedium")));
        value.put("Volume", seri(ele.elementText("Volume")));
        value.put("Issue", seri(ele.elementText("Issue")));
        value.put("PubDate", tranPubDate(ele.element("PubDate"), schemaPath + File.separatorChar + "PubDate"));
        return value;
    }

    static Record tranPubDate(Element ele, String schemaPath) throws IOException {
        if (sPubDate == null)
            sPubDate = new Schema.Parser().parse(new File(schemaPath + File.separatorChar + "PubDate.avsc"));
        Record value = new Record(sPubDate);
        value.put("Year", seri(ele.elementText("Year")));
        value.put("Season", seri(ele.elementText("Season")));
        value.put("Month", seri(ele.elementText("Month")));
        value.put("Day", seri(ele.elementText("Day")));
        value.put("MedlineDate", seri(ele.elementText("MedlineDate")));
        return value;
    }

    static Record tranELocationID(Element ele, String schemaPath) throws IOException {
        if (sELocationID == null)
            sELocationID = new Schema.Parser().parse(new File(schemaPath + File.separatorChar + "ELocationID.avsc"));
        Record value = new Record(sELocationID);
        value.put("EIdType", seri(ele.attributeValue("EIdType")));
        value.put("EValidYN", seri(ele.attributeValue("EValidYN")));
        value.put("EId", ele.getStringValue());
        return value;
    }

    static List<Record> tranAbstract(Element ele, String schemaPath) throws IOException {
        List<Record> value = new ArrayList<Record>();
        if (ele != null) {
            Iterator it = ele.elementIterator("AbstractText");
            String path = schemaPath + File.separatorChar + "AbstractText";
            while (it.hasNext()) {
                value.add(tranAbstractText((Element) it.next(), path));
            }
        }
        return value;
    }

    static Record tranAbstractText(Element ele, String schemaPath) throws IOException {
        if (sAbstractText == null)
            sAbstractText = new Schema.Parser().parse(new File(schemaPath + File.separatorChar + "AbstractText.avsc"));
        Record value = new Record(sAbstractText);
        value.put("Label", seri(ele.attributeValue("Label")));
        value.put("NlmCategory", seri(ele.attributeValue("NlmCategory")));
        value.put("aText", ele.getStringValue());
        return value;
    }

    static List<Record> tranAuthorList(Element ele, String schemaPath) throws IOException {
        List<Record> value = new ArrayList<Record>();
        if (ele != null) {
            Iterator it = ele.elementIterator("Author");
            String path = schemaPath + File.separatorChar + "Author";
            while (it.hasNext()) {
                value.add(tranAuthor((Element) it.next(), path));
            }
        }
        return value;
    }

    static Record tranAuthor(Element ele, String schemaPath) throws IOException {
        if (sAuthor == null)
            sAuthor = new Schema.Parser().parse(new File(schemaPath + File.separatorChar + "Author.avsc"));
        Record value = new Record(sAuthor);
        value.put("AuValidYN", seri(ele.attributeValue("ValidYN")));
        value.put("AuLastName", seri(ele.elementText("LastName")));
        value.put("AuForeName", seri(ele.elementText("ForeName")));
        value.put("AuInitials", seri(ele.elementText("Initials")));
        Element e = ele.element("AffilicationInfo");
        if (e != null)
            value.put("AuAffilication", seri(e.elementText("Affilication")));
        else
            value.put("AuAffilication", seri(null));
        value.put("Suffix", seri(ele.elementText("Suffix")));
        value.put("CollectiveName", seri(ele.elementText("CollectiveName")));
        e = ele.element("Identifier");
        if (e != null) {
            value.put("IdenSource", seri(e.attributeValue("Source")));
            value.put("Identifier", seri(e.getStringValue()));
        } else {
            value.put("IdenSource", seri(null));
            value.put("Identifier", seri(null));
        }
        return value;
    }

    static List<Record> tranDataBankList(Element ele, String schemaPath) throws IOException {
        List<Record> value = new ArrayList<Record>();
        if (ele != null) {
            Iterator it = ele.elementIterator("DataBank");
            String path = schemaPath + File.separatorChar + "DataBank";
            while (it.hasNext()) {
                value.add(tranDataBank((Element) it.next(), path));
            }
        }
        return value;
    }

    static Record tranDataBank(Element ele, String schemaPath) throws IOException {
        if (sDataBank == null)
            sDataBank = new Schema.Parser().parse(new File(schemaPath + File.separatorChar + "DataBank.avsc"));
        Record value = new Record(sDataBank);
        value.put("DataBankName", seri(ele.elementText("DataBankName")));
        value.put("AccessionNumberList", tranAccessionNumberList(ele.element("AccessionNumberList"),
                schemaPath + File.separatorChar + "AccessionNumberList"));
        return value;
    }

    static List<String> tranAccessionNumberList(Element ele, String schemaPath) throws IOException {
        List<String> value = new ArrayList<String>();
        if (ele != null) {
            Iterator it = ele.elementIterator("AccessionNumber");
            String path = schemaPath + File.separatorChar + "AccessionNumber";
            while (it.hasNext()) {
                value.add(((Element) it.next()).getStringValue());
            }
        }
        return value;
    }

    static List<Record> tranGrantList(Element ele, String schemaPath) throws IOException {
        List<Record> value = new ArrayList<Record>();
        if (ele != null) {
            Iterator it = ele.elementIterator("Grant");
            String path = schemaPath + File.separatorChar + "Grant";
            while (it.hasNext()) {
                value.add(tranGrant((Element) it.next(), path));
            }
        }
        return value;
    }

    static Record tranGrant(Element ele, String schemaPath) throws IOException {
        if (sGrant == null)
            sGrant = new Schema.Parser().parse(new File(schemaPath + File.separatorChar + "Grant.avsc"));
        Record value = new Record(sGrant);
        value.put("GrantID", seri(ele.elementText("GrantID")));
        value.put("Acroym", seri(ele.elementText("Acroym")));
        value.put("Agency", seri(ele.elementText("Agency")));
        value.put("GrCountry", seri(ele.elementText("Country")));
        return value;
    }

    static List<Record> tranPublicationTypeList(Element ele, String schemaPath) throws IOException {
        List<Record> value = new ArrayList<Record>();
        Iterator it = ele.elementIterator("PublicationType");
        String path = schemaPath + File.separatorChar + "PublicationType";
        while (it.hasNext()) {
            value.add(tranPublicationType((Element) it.next(), path));
        }
        return value;
    }

    static Record tranPublicationType(Element ele, String schemaPath) throws IOException {
        if (sPublicationType == null)
            sPublicationType = new Schema.Parser()
                    .parse(new File(schemaPath + File.separatorChar + "PublicationType.avsc"));
        Record value = new Record(sPublicationType);
        value.put("PTUI", seri(ele.attributeValue("UI")));
        value.put("publicationType", seri(ele.getStringValue()));
        return value;
    }

    static Record tranArticleDate(Element ele, String schemaPath) throws IOException {
        if (sArticleDate == null)
            sArticleDate = new Schema.Parser().parse(new File(schemaPath + File.separatorChar + "ArticleDate.avsc"));
        Record value = new Record(sArticleDate);
        if (ele != null) {
            value.put("DateType", seri(ele.attributeValue("DateType")));
            String date = ele.elementText("Year") + "-"
                    + String.format("%02d", Integer.parseInt(ele.elementText("Month").trim())) + "-"
                    + String.format("%02d", Integer.parseInt(ele.elementText("Day").trim()));
            value.put("articleDate", date);
        } else {
            value.put("DateType", seri(null));
            //            String date = ele.elementText("Year") + "-"
            //                    + String.format("%02d", Integer.parseInt(ele.elementText("Month"))) + "-"
            //                    + String.format("%02d", Integer.parseInt(ele.elementText("Day")));
            value.put("articleDate", seri(null));
        }
        return value;
    }

    static Record tranMedlineJournalInfo(Element ele, String schemaPath) throws IOException {
        if (sMedlineJournalInfo == null)
            sMedlineJournalInfo = new Schema.Parser()
                    .parse(new File(schemaPath + File.separatorChar + "MedlineJournalInfo.avsc"));
        Record value = new Record(sMedlineJournalInfo);
        value.put("MJCountry", seri(ele.elementText("Country")));
        value.put("MedlineTA", seri(ele.elementText("MedlineTA")));
        value.put("NlmUniqueID", seri(ele.elementText("NlmUniqueID")));
        value.put("ISSNLinking", seri(ele.elementText("ISSNLinking")));
        return value;
    }

    static List<Record> tranChemicalList(Element ele, String schemaPath) throws IOException {
        List<Record> value = new ArrayList<Record>();
        if (ele != null) {
            Iterator it = ele.elementIterator("Chemical");
            String path = schemaPath + File.separatorChar + "Chemical";
            while (it.hasNext()) {
                value.add(tranChemical((Element) it.next(), path));
            }
        }
        return value;
    }

    static Record tranChemical(Element ele, String schemaPath) throws IOException {
        if (sChemical == null)
            sChemical = new Schema.Parser().parse(new File(schemaPath + File.separatorChar + "Chemical.avsc"));
        Record value = new Record(sChemical);
        value.put("RegistryNumber", seri(ele.elementText("RegistryNumber")));
        value.put("NameOfSubstance", tranNameOfSubstance(ele.element("NameOfSubstance"),
                schemaPath + File.separatorChar + "NameOfSubstance"));
        return value;
    }

    static Record tranNameOfSubstance(Element ele, String schemaPath) throws IOException {
        if (sNameOfSubstance == null)
            sNameOfSubstance = new Schema.Parser()
                    .parse(new File(schemaPath + File.separatorChar + "NameOfSubstance.avsc"));
        Record value = new Record(sNameOfSubstance);
        value.put("NSUI", seri(ele.attributeValue("UI")));
        value.put("nameOfSubstance", ele.getStringValue());
        return value;
    }

    static List<Record> tranSupplMeshList(Element ele, String schemaPath) throws IOException {
        List<Record> value = new ArrayList<Record>();
        if (ele != null) {
            Iterator it = ele.elementIterator("SupplMeshName");
            String path = schemaPath + File.separatorChar + "SupplMeshName";
            while (it.hasNext()) {
                value.add(tranSupplMeshName((Element) it.next(), path));
            }
        }
        return value;
    }

    static Record tranSupplMeshName(Element ele, String schemaPath) throws IOException {
        if (sSupplMeshName == null)
            sSupplMeshName = new Schema.Parser()
                    .parse(new File(schemaPath + File.separatorChar + "SupplMeshName.avsc"));
        Record value = new Record(sSupplMeshName);
        value.put("SMType", seri(ele.attributeValue("Type")));
        value.put("SMUI", seri(ele.attributeValue("UI")));
        value.put("supplMeshName", ele.getStringValue());
        return value;
    }

    static List<String> tranGeneSymbolList(Element ele, String schemaPath) throws IOException {
        List<String> value = new ArrayList<String>();
        if (ele != null) {
            Iterator it = ele.elementIterator("GeneSymbol");
            while (it.hasNext()) {
                value.add(((Element) it.next()).getStringValue());
            }
        }
        return value;
    }

    static List<Record> tranCommentsCorrectionsList(Element ele, String schemaPath) throws IOException {
        List<Record> value = new ArrayList<Record>();
        if (ele != null) {
            Iterator it = ele.elementIterator("CommentsCorrections");
            String path = schemaPath + File.separatorChar + "CommentsCorrections";
            while (it.hasNext()) {
                value.add(tranCommentsCorrections((Element) it.next(), path));
            }
        }
        return value;
    }

    static Record tranCommentsCorrections(Element ele, String schemaPath) throws IOException {
        if (sCommentsCorrections == null)
            sCommentsCorrections = new Schema.Parser()
                    .parse(new File(schemaPath + File.separatorChar + "CommentsCorrections.avsc"));
        Record value = new Record(sCommentsCorrections);
        value.put("RefType", seri(ele.attributeValue("RefType")));
        value.put("RefSource", seri(ele.elementText("RefSource")));
        value.put("CCPMID", tranCCPMID(ele.element("PMID"), schemaPath + File.separatorChar + "CCPMID"));
        value.put("Note", seri(ele.elementText("Note")));
        return value;
    }

    static Record tranCCPMID(Element ele, String schemaPath) throws IOException {
        if (sCCPMID == null)
            sCCPMID = new Schema.Parser().parse(new File(schemaPath + File.separatorChar + "CCPMID.avsc"));
        Record value = new Record(sCCPMID);
        if (ele != null) {
            value.put("CCVersion", seri(ele.attributeValue("Version")));
            value.put("CCpmid", Integer.parseInt(ele.getStringValue().trim()));
        } else {
            value.put("CCVersion", seri(null));
            value.put("CCpmid", seriInteger(null));
        }
        return value;
    }

    static List<Record> tranMeshHeadingList(Element ele, String schemaPath) throws IOException {
        List<Record> value = new ArrayList<Record>();
        if (ele != null) {
            Iterator it = ele.elementIterator("MeshHeading");
            String path = schemaPath + File.separatorChar + "MeshHeading";
            while (it.hasNext()) {
                value.add(tranMeshHeading((Element) it.next(), path));
            }
        }
        return value;
    }

    static Record tranMeshHeading(Element ele, String schemaPath) throws IOException {
        if (sMeshHeading == null)
            sMeshHeading = new Schema.Parser().parse(new File(schemaPath + File.separatorChar + "MeshHeading.avsc"));
        Record value = new Record(sMeshHeading);
        value.put("DescriptorName",
                tranDescriptorName(ele.element("DescriptorName"), schemaPath + File.separatorChar + "DescriptorName"));
        Iterator it = ele.elementIterator("QualifierName");
        List<Record> v = new ArrayList<Record>();
        while (it.hasNext()) {
            v.add(tranQualifierName((Element) it.next(),
                    schemaPath + File.separatorChar + "QualifierNameList" + File.separatorChar + "QualifierName"));
        }
        value.put("QualifierNameList", v);
        return value;
    }

    static Record tranDescriptorName(Element ele, String schemaPath) throws IOException {
        if (sDescriptorName == null)
            sDescriptorName = new Schema.Parser()
                    .parse(new File(schemaPath + File.separatorChar + "DescriptorName.avsc"));
        Record value = new Record(sDescriptorName);
        value.put("DNUI", seri(ele.attributeValue("UI")));
        value.put("DNMajorTopicYN", seri(ele.attributeValue("MajorTopicYN")));
        value.put("descriptorName", ele.getStringValue());
        value.put("DNType", seri(ele.attributeValue("Type")));
        return value;
    }

    static Record tranQualifierName(Element ele, String schemaPath) throws IOException {
        if (sQualifierName == null)
            sQualifierName = new Schema.Parser()
                    .parse(new File(schemaPath + File.separatorChar + "QualifierName.avsc"));
        Record value = new Record(sQualifierName);
        value.put("QNUI", seri(ele.attributeValue("UI")));
        value.put("QNMajorTopicYN", seri(ele.attributeValue("MajorTopicYN")));
        value.put("qualifierName", ele.getStringValue());
        return value;
    }

    static Record tranOtherID(Element ele, String schemaPath) throws IOException {
        if (sOtherID == null)
            sOtherID = new Schema.Parser().parse(new File(schemaPath + File.separatorChar + "OtherID.avsc"));
        Record value = new Record(sOtherID);
        value.put("Source", seri(ele.attributeValue("Source")));
        value.put("otherID", ele.getStringValue());
        return value;
    }

    static Record tranOtherAbstract(Element ele, String schemaPath) throws IOException {
        if (sOtherAbstract == null)
            sOtherAbstract = new Schema.Parser()
                    .parse(new File(schemaPath + File.separatorChar + "OtherAbstract.avsc"));
        Record value = new Record(sOtherAbstract);
        if (ele != null) {
            value.put("OAType", seri(ele.attributeValue("Type")));
            value.put("OALanguage", seri(ele.attributeValue("Language")));
            value.put("OAbstractText", seri(ele.elementText("AbstractText")));
        } else {
            value.put("OAType", seri(null));
            value.put("OALanguage", seri(null));
            value.put("OAbstractText", seri(null));
        }
        return value;
    }

    static List<Record> tranKeywordList(Element ele, String schemaPath) throws IOException {
        List<Record> value = new ArrayList<Record>();
        if (ele != null) {
            Iterator it = ele.elementIterator("Keyword");
            String path = schemaPath + File.separatorChar + "Keyword";
            while (it.hasNext()) {
                value.add(tranKeyword((Element) it.next(), path));
            }
        }
        return value;
    }

    static Record tranKeyword(Element ele, String schemaPath) throws IOException {
        if (sKeyword == null)
            sKeyword = new Schema.Parser().parse(new File(schemaPath + File.separatorChar + "Keyword.avsc"));
        Record value = new Record(sKeyword);
        value.put("KMajorTopicYN", seri(ele.attributeValue("MajorTopicYN")));
        value.put("keyword", ele.getStringValue());
        return value;
    }

    static List<Record> tranInvestigatorList(Element ele, String schemaPath) throws IOException {
        List<Record> value = new ArrayList<Record>();
        if (ele != null) {
            Iterator it = ele.elementIterator("Investigator");
            String path = schemaPath + File.separatorChar + "Investigator";
            while (it.hasNext()) {
                value.add(tranInvestigator((Element) it.next(), path));
            }
        }
        return value;
    }

    static Record tranInvestigator(Element ele, String schemaPath) throws IOException {
        if (sInvestigator == null)
            sInvestigator = new Schema.Parser().parse(new File(schemaPath + File.separatorChar + "Investigator.avsc"));
        Record value = new Record(sInvestigator);
        value.put("InValidYN", seri(ele.attributeValue("ValidYN")));
        value.put("InLastName", seri(ele.elementText("LastName")));
        value.put("InForeName", seri(ele.elementText("ForeName")));
        value.put("InInitials", seri(ele.elementText("Initials")));
        Element e = ele.element("AffiliationInfo");
        if (e != null)
            value.put("InAffiliation", seri(e.elementText("Affiliation")));
        else
            value.put("InAffiliation", seri(null));
        return value;
    }

    static Record tranGeneralNote(Element ele, String schemaPath) throws IOException {
        if (sGeneralNote == null)
            sGeneralNote = new Schema.Parser().parse(new File(schemaPath + File.separatorChar + "GeneralNote.avsc"));
        Record value = new Record(sGeneralNote);
        if (ele != null) {
            value.put("GNOwner", seri(ele.attributeValue("Owner")));
            value.put("generalNote", ele.getStringValue());
        } else {
            value.put("GNOwner", seri(null));
            value.put("generalNote", seri(null));
        }
        return value;
    }

    static List<Record> tranPersonalNameSubjectList(Element ele, String schemaPath) throws IOException {
        List<Record> value = new ArrayList<Record>();
        if (ele != null) {
            Iterator it = ele.elementIterator("PersonalNameSubject");
            String path = schemaPath + File.separatorChar + "PersonalNameSubject";
            while (it.hasNext()) {
                value.add(tranPersonalNameSubject((Element) it.next(), path));
            }
        }
        return value;
    }

    static Record tranPersonalNameSubject(Element ele, String schemaPath) throws IOException {
        if (sPersonalNameSubject == null)
            sPersonalNameSubject = new Schema.Parser()
                    .parse(new File(schemaPath + File.separatorChar + "PersonalNameSubject.avsc"));
        Record value = new Record(sPersonalNameSubject);
        value.put("PSLastName", seri(ele.elementText("LastName")));
        value.put("PSForeName", seri(ele.elementText("ForeName")));
        value.put("PSInitials", seri(ele.elementText("Initials")));
        return value;
    }

    static Record tranPubmedData(Element ele, String schemaPath) throws IOException {
        if (sPubmedData == null)
            sPubmedData = new Schema.Parser().parse(new File(schemaPath + File.separatorChar + "PubmedData.avsc"));
        Record value = new Record(sPubmedData);
        value.put("History", tranHistory(ele.element("History"), schemaPath + File.separatorChar + "History"));
        value.put("PublicationStatus", seri(ele.elementText("PublicationStatus")));
        value.put("ArticleIdList",
                tranArticleIdList(ele.element("ArticleIdList"), schemaPath + File.separatorChar + "ArticleIdList"));
        return value;
    }

    static List<Record> tranHistory(Element ele, String schemaPath) throws IOException {
        List<Record> value = new ArrayList<Record>();
        Iterator it = ele.elementIterator("PubmedPubDate");
        String path = schemaPath + File.separatorChar + "PubmedPubDate";
        while (it.hasNext()) {
            value.add(tranPubmedPubDate((Element) it.next(), path));
        }
        return value;
    }

    static Record tranPubmedPubDate(Element ele, String schemaPath) throws IOException {
        if (sPubmedPubDate == null)
            sPubmedPubDate = new Schema.Parser()
                    .parse(new File(schemaPath + File.separatorChar + "PubmedPubDate.avsc"));
        Record value = new Record(sPubmedPubDate);
        value.put("PubStatus", seri(ele.attributeValue("PubStatus")));
        String date = null;
        String time = null;
        date = ele.elementText("Year") + "-" + String.format("%02d", Integer.parseInt(ele.elementText("Month").trim()))
                + "-" + String.format("%02d", Integer.parseInt(ele.elementText("Day").trim()));
        if (ele.element("Hour") != null)
            time = String.format("%02d", Integer.parseInt(ele.elementText("Hour").trim())) + ":"
                    + String.format("%02d", Integer.parseInt(ele.elementText("Minute").trim()));
        else
            time = seri(null);
        value.put("pubmedPubDate", date);
        value.put("pubmedPubTime", time);
        return value;
    }

    static List<Record> tranArticleIdList(Element ele, String schemaPath) throws IOException {
        List<Record> value = new ArrayList<Record>();
        Iterator it = ele.elementIterator("ArticleId");
        String path = schemaPath + File.separatorChar + "ArticleId";
        while (it.hasNext()) {
            value.add(tranArticleId((Element) it.next(), path));
        }
        return value;
    }

    static Record tranArticleId(Element ele, String schemaPath) throws IOException {
        if (sArticleId == null)
            sArticleId = new Schema.Parser().parse(new File(schemaPath + File.separatorChar + "ArticleId.avsc"));
        Record value = new Record(sArticleId);
        value.put("IdType", seri(ele.attributeValue("IdType")));
        value.put("articleId", ele.getStringValue());
        return value;
    }

    static String seri(String ele) {
        if (ele == null)
            return "";
        else
            return ele;
    }

    static int seriInteger(String ele) {
        if (ele == null)
            return -1;
        else
            return Integer.parseInt(ele.trim());
    }

    public static void main(String[] args) {
        String path = args[0];
        File[] files = new File(path).listFiles();
        String schema = args[1];
        String resultPath = args[2];
        int free = Integer.parseInt(args[3]);
        int mul = Integer.parseInt(args[4]);
        //        try {
        //            FileInputStream fis;
        //            InputStreamReader re;
        //            fis = new FileInputStream(path);
        //            re = new InputStreamReader(fis, "UTF-8");
        tranPubmedArticleSet(files, schema, resultPath, free, mul);
        //            re.close();
        //            fis.close();
        //        } 
        //        catch (FileNotFoundException e) {
        //            e.printStackTrace();
        //        } catch (UnsupportedEncodingException e) {
        //            e.printStackTrace();
        //        } catch (IOException e) {
        //            e.printStackTrace();
        //        }
    }
}
