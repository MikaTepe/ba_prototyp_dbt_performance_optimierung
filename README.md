Kurzanleitung: Silvershot-Prototyp für temporale Silver-Layer-Transformationen

1. Zweck des Prototyps
Der aktuelle Prototyp unterstützt temporale Transformationen für Silver-Layer-Tabellen. Abgedeckt werden aktuell zwei Historisierungsarten:
•	unitemporale Historisierung
•	bitemporale Historisierung
Die Transformationen werden über eine custom dbt-Materialisierung namens Silvershot ausgeführt. Die Zieltabellen und die zugehörigen temporären Tabellen müssen bereits existieren. dbt übernimmt in diesem Prototyp nicht die Erstellung neuer Artefakte.

2. Modell-Template
Die Datei Silvershot_Template enthält einen vorbereiteten config-Block mit allen notwendigen Parametern, damit ein Modell durch den Prototyp ausgeführt werden kann.

Parameterbeschreibung
unique_key=[]
Hier werden die Spalten angegeben, die gemeinsam den fachlichen Schlüssel, also den Business Key, bilden.

Beispiel:
unique_key=['konto_id', 'przs_id_ins'],

strategy=''
Dieser Parameter legt fest, welche Historisierungslogik verwendet wird.
Erlaubte Werte:
strategy='unitemporal'

oder
strategy='bitemporal'





temporal_cols={"von": "", "bis": ""}
Hier werden die Namen der fachlichen Gültigkeitsspalten angegeben.
Beispiel:
temporal_cols={
            "von": "von",
            "bis": "bis"
        }


temporal_cols={
            "von": "tech_ats",
            "bis": "tech_ets"
        }

temporal_value=''
Dieser Parameter definiert den Wert, der bei Änderungen zur Aktivierung oder Deaktivierung von Datensätzen verwendet wird.
Unterstützte Möglichkeiten:
temporal_value='CURRENT_TIMESTAMP'
temporal_value='CURRENT_DATE'
temporal_value='VAR("BDAT")'

Wenn VAR("BDAT") verwendet wird, muss beim Ausführen des Prozesses ein BDAT-Wert übergeben werden.

temporal_value='CURRTS'

CURRTS verwendet den Aktivierungszeitpunkt der Transformation global für alle Datensätze des Prozesslaufs.
temporal_value='TO_DATE("2026-04-23")'

Zusätzlich kann ULTIMO(<VALUE>) verwendet werden. Dabei wird aus dem übergebenen Wert der Monatsultimo abgeleitet.
Beispiele:
temporal_value='ULTIMO(CURRENT_DATE)'
temporal_value='ULTIMO(CURRENT_TIMESTAMP)'
temporal_value='ULTIMO(CURRTS)'
temporal_value='ULTIMO(TO_DATE("2026-04-23"))'
temporal_value='ULTIMO(VAR("BDAT"))'
Wichtig: Bei CURRENT_DATE, CURRENT_TIMESTAMP und CURRTS innerhalb von ULTIMO(...) dürfen keine zusätzlichen Anführungszeichen verwendet werden.
meta={...}
Im meta-Block werden Metadaten zum Transformationsmodell gespeichert.
meta={
            "INSERT_STATEMENT":"“,
            "TAB_FKEY":"",
            "RELEASE_FKEY":""
        }

Bedeutung:
•	INSERT_STATEMENT: Name des ELT beziehungsweise des dbt-Modells
•	TAB_FKEY: Identifier der Zieltabelle
•	RELEASE_FKEY: Release-Nummer des Transformationsmodells
RELEASE_FKEY wird aktuell noch nicht funktional verwendet. Die spätere Nutzung ist für die Integration mit der Deployment-Pipeline vorgesehen.

3. Schreiben der Transformation
Nach dem Setzen der Konfigurationswerte kann die Transformation direkt mit dem SELECT-Statement begonnen werden.
Ein INSERT INTO ist nicht notwendig.
Beispiel:
select
        idh_przs_id_ins,
        gut_last_merk
    from {{ source('local_lakehouse', 'rds_grundkonto_az1') }} az1
    where szsa_art = 'E'
      and az1.idh_gltg_fach_adtm <= CURRENT_DATE
      and az1.idh_gltg_fach_edtm > CURRENT_DATE

Der Insert- und Merge-Prozess wird durch die Silvershot-Materialisierung gesteuert.
4. Voraussetzung: 
Temp Table
Für jede Zieltabelle, die mit dem Prototyp verarbeitet werden soll, muss eine passende temporäre Tabelle existieren.
Die Temp Table muss folgende Struktur haben:
•	alle Spalten der Zieltabelle
•	zusätzlich die Spalte change_flag_col

Sources


5. Ausführen der Transformation
Die Transformation kann auf drei Arten ausgeführt werden.
Variante 1: Direkt über das Terminal
Zuerst muss die Python Virtual Environment aktiviert sein.
Danach kann ein Modell direkt mit dbt ausgeführt werden:
dbt run --select <model_name> --vars '{"<variable_name>":"<variable_value>"}'

Beispiel:
dbt run --select INSERT_IAM_I_GRUNDKONTO_AZ1_U --vars '{"INR_FKEY":"j548910"}'

Beispiel mit BDAT:
dbt run --select INSERT_IAM_I_GRUNDKONTO_AZ1_U --vars '{"INR_FKEY":"j548910",”BDAT”:”2026-04-24”}'

Für detailliertes Debugging kann zusätzlich --debug verwendet werden:
dbt run --select INSERT_IAM_I_GRUNDKONTO_AZ1_U --vars '{"INR_FKEY":"j548910"}' --debug

Variante 2: Ausführung über trigger_silver_dbt.py
Der Prozess kann auch über das Python-Trigger-Skript gestartet werden:
python trigger_silver_dbt.py --insert_statement <model_name> --inr-fkey <schema_name>

Optional kann ein BDAT übergeben werden:
python trigger_silver_dbt.py --insert_statement <model_name> --inr-fkey <schema_name> --bdat <bdat_value>

Beispiel:
python trigger_silver_dbt.py --insert-statement INSERT_IAM_I_GRUNDKONTO_AZ1_U --inr_fkey j548910




Variante 3: Ausführung über die GUI
Die GUI kann mit folgendem Befehl gestartet werden:
python trigger_silver_dbt_gui.py

Nach dem Start erscheint ein Fenster mit drei Eingabeblöcken:
•	INSERT_STATEMENT
•	INR_FKEY
•	BDAT optional
Zusätzlich enthält die GUI:
•	einen Run-Button
•	eine Statusanzeige
•	einen ausklappbaren Logging-Bereich
Nach dem Eintragen der benötigten Werte kann der Prozess über den Run-Button gestartet werden. Während der Ausführung zeigt die GUI den laufenden Status an.
Nach Abschluss des Prozesses wird angezeigt, ob die Transformation erfolgreich war oder fehlgeschlagen ist. Über den Log-Bereich kann das vollständige Prozess-Log eingesehen werden.
Ein weiterer Lauf kann direkt gestartet werden, indem die Eingabewerte angepasst und erneut der Run-Button betätigt wird.




Für die Verwendung der Snapshot Materialisierung wird folgender CLI Befehlt benötigt:

dbt snapshot --select <Snapshot-Model Name> --vars '{"INR_FKEY":"<Zielrelationsschema>","TAB_FKEY":"<Zielrelation Identifier>","active_snapshot":"<Snapshot-Model Name>"}' --debug

Bitte verwenden Sie die zur Verfügung gestellte Vorlage 'snapshot_vorlage' zur Verwendung der Snapshot Funktionalität.

-- LOAD_FILTER + fusi_quel_inst_schl Bug Fix 12.05.2026

Ab jetzt muss fusi_quel_inst_schl für neu ankommende Datensätze explizit im ELT eingesetzt werden. Diese Spalte wird nicht automatisch vom Framework verarbeitet.

LOAD_FILTER ist ab dem Punkt nutzbar. Mit LOAD_FILTER darf man jene Art von WHERE Bedingung zum Transformations-SQL injezieren. Beispiel:

dbt snapshot --select INSERT_SILVER_KUNDEN_KONTO_STATUS --vars '{"INR_FKEY":"j548910","TAB_FKEY":"silver_kunden_konto_status","LOAD_FILTER":"risiko_klasse in (’LOW’, ’MEDIUM’)","active_snapshot":"INSERT_SILVER_KUNDEN_KONTO_STATUS"}' --debug

Oder

dbt snapshot --select INSERT_SILVER_KUNDEN_KONTO_STATUS --vars '{"INR_FKEY":"j548910","TAB_FKEY":"silver_kunden_konto_status","LOAD_FILTER":"kunden_segment <> ’VIP’","active_snapshot":"INSERT_SILVER_KUNDEN_KONTO_STATUS"}' --debug

Es dürfen auch mehrere WHERE Bedingungen gleichzeitig injeziert werden:

dbt snapshot --select INSERT_SILVER_KUNDEN_KONTO_STATUS --vars '{"INR_FKEY":"j548910","TAB_FKEY":"silver_kunden_konto_status","LOAD_FILTER":"risiko_klasse in (’LOW’, ’MEDIUM’) and kunden_segment <> ’VIP’","active_snapshot":"INSERT_SILVER_KUNDEN_KONTO_STATUS"}' --debug