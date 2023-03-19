from nr_oaipmh_harvesters.nusl.transformer import NUSLTransformer, vocabulary_cache
from oarepo_oaipmh_harvester.readers.oai_dir import OAIDirReader
from nr_metadata.documents.services.records.schema import NRDocumentRecordSchema
import tqdm
import json
from invenio_app.factory import create_app
import csv


def run():
    app = create_app()
    with app.app_context():
        data = []
        for inst in institutions.strip().split("\n"):
            inst = inst.strip()
            inst = inst.rsplit(" ", maxsplit=1)[0]
            print()
            print(inst)
            resolved = vocabulary_cache.get_institution(inst)
            print("    ", resolved["id"] if resolved else "---")
            data.append((inst, resolved["id"] if resolved else ""))
        with open("/tmp/instituce.csv", "w") as f:
            writer = csv.writer(f)
            writer.writerows(data)


institutions = """
Vysoká škola zemědělská, Agronomická fakulta 1
Vysoká škola zemědělská v Brně, Zootechnická fakulta 1
 Akademie múzických umění v Praze. Divadelní fakulta AMU 589
', Agronomická a zootechnická fakulta' 25
 ', Agronomická fakulta' 4
 ', Lednice na Moravě' 1
 ', Lesnická fakulta' 1
 ', Technická fakulta CZU' 1
 ', Ústav agrikulturní chemie' 1
 ', Ústav agrochemický' 1
 ', Ústav použité entomologie, ochrany lesa a myslivosti' 1
 Akademie múzických umění v Praze. Filmová a televizní fakulta AMU 584
 Akademie múzických umění v Praze. Hudební a taneční fakulta AMU 686
 Akademie múzických umění v Praze. Hudební fakulta AMU 142
 Akademie múzických umění v Praze.Divadelní fakulta 651
 Akademie múzických umění v Praze.Filmová a televizní fakulta 633
 Akademie múzických umění v Praze.Hudební a taneční fakulta 1004
 Akademie věd České republiky 2
 JIHOČESKÁ UNIVERZITA V ČESKÝCH BUDĚJOVICÍCH 40598
 Masarykova univerzita 1
 Mendelova univerzita (Brno), Fakulta Institut celoživotního vzdělávání 6
 Mendelova univerzita (Brno), Fakulta lesnická a dřevařská 138
 Mendelova univerzita (Brno), Fakulta provozně ekonomická 109
 Mendelova univerzita 223
 Mendelova univerzita v Brně, Agronomická fakulta 10
 Mendelova univerzita v Brně. Zahradnická fakulta 1
 Mendelova univerzita, Agronomická fakulta 6808
 Mendelova univerzita, Fakulta agronomická 1
 Mendelova univerzita, Fakulta regionálního rozvoje a mezinárodních studií 1898
 Mendelova univerzita, Institut celoživotního vzdělávání 586
 Mendelova univerzita, Lesnická a dřevařská fakulta 3968
 Mendelova univerzita, Provozně ekonomická fakulta 7270
 Mendelova univerzita, Zahradnická fakulta 1857
 Mendelova zemědělská a lesnická univerzita (Brno), Fakulta zahradnická 81
 Mendelova zemědělská a lesnická univerzita 244
 Mendelova zemědělská a lesnická univerzita v Brně, Lesnická a dřevařská 1
 Mendelova zemědělská a lesnická univerzita, Agronomická fakulta 1847
 Mendelova zemědělská a lesnická univerzita, Fakulta agronomická 6
 Mendelova zemědělská a lesnická univerzita, Fakulta provozně ekonomická 2
 Mendelova zemědělská a lesnická univerzita, Fakulta zahradnická 1
 Mendelova zemědělská a lesnická univerzita, Institut celoživotního vzdělávání 25
 Mendelova zemědělská a lesnická univerzita, Laboratoř molekulární embryologie 1
 Mendelova zemědělská a lesnická univerzita, Lesnická a dřevařská fakulta 1131
 Mendelova zemědělská a lesnická univerzita, Provozně ekonomická fakulta 1744
 Mendelova zemědělská a lesnická univerzita, Ústav agrochemie, půdoznalství, 8
 Mendelova zemědělská a lesnická univerzita, Ústav agrosystémů a bioklimatologie 2
 Mendelova zemědělská a lesnická univerzita, Ústav aplikované a krajinné 8
 Mendelova zemědělská a lesnická univerzita, Ústav biologie rostlin 4
 Mendelova zemědělská a lesnická univerzita, Ústav biotechniky zeleně 3
 Mendelova zemědělská a lesnická univerzita, Ústav botaniky a fyziologie 2
 Mendelova zemědělská a lesnická univerzita, Ústav chemie a biochemie 7
 Mendelova zemědělská a lesnická univerzita, Ústav chovu a šlechtění zvířat 9
 Mendelova zemědělská a lesnická univerzita, Ústav chovu hospodářských 8
 Mendelova zemědělská a lesnická univerzita, Ústav ekologie lesa 2
 Mendelova zemědělská a lesnická univerzita, Ústav ekonomie 3
 Mendelova zemědělská a lesnická univerzita, Ústav genetiky 1
 Mendelova zemědělská a lesnická univerzita, Ústav geologie a pedologie 1
 Mendelova zemědělská a lesnická univerzita, Ústav hospodářské úpravy lesů 3
 Mendelova zemědělská a lesnická univerzita, Ústav informatiky a operační 1
 Mendelova zemědělská a lesnická univerzita, Ústav lesnické a dřevařské 2
 Mendelova zemědělská a lesnická univerzita, Ústav lesnické botaniky, dendrologie 11
 Mendelova zemědělská a lesnická univerzita, Ústav managementu 4
 Mendelova zemědělská a lesnická univerzita, Ústav marketingu a obchodu 2
 Mendelova zemědělská a lesnická univerzita, Ústav molekulární embryologie 1
 Mendelova zemědělská a lesnická univerzita, Ústav morfologie, fyziologie 11
 Mendelova zemědělská a lesnická univerzita, Ústav nábytku, designu a bydlení 6
 Mendelova zemědělská a lesnická univerzita, Ústav nauky o dřevě 4
 Mendelova zemědělská a lesnická univerzita, Ústav obecné produkce rostlinné 1
 Mendelova zemědělská a lesnická univerzita, Ústav ochrany lesů a myslivosti 6
 Mendelova zemědělská a lesnická univerzita, Ústav ochrany rostlin 2
 Mendelova zemědělská a lesnická univerzita, Ústav ovocnictví 1
 Mendelova zemědělská a lesnická univerzita, Ústav pícninářství 1
 Mendelova zemědělská a lesnická univerzita, Ústav podnikové ekonomiky 3
 Mendelova zemědělská a lesnická univerzita, Ústav půdoznalství a mikrobiologie 2
 Mendelova zemědělská a lesnická univerzita, Ústav pěstování a šlechtění 5
 Mendelova zemědělská a lesnická univerzita, Ústav pěstování, šlechtění 3
 Mendelova zemědělská a lesnická univerzita, Ústav rybářství a hydrobiologie 1
 Mendelova zemědělská a lesnická univerzita, Ústav statistiky a operačního 2
 Mendelova zemědělská a lesnická univerzita, Ústav techniky a automobilové 5
 Mendelova zemědělská a lesnická univerzita, Ústav technologie potravin 8
 Mendelova zemědělská a lesnická univerzita, Ústav tvorby a ochrany krajiny 5
 Mendelova zemědělská a lesnická univerzita, Ústav účetnictví a daní 6
 Mendelova zemědělská a lesnická univerzita, Ústav výživy a krmení hospodářských 3
 Mendelova zemědělská a lesnická univerzita, Ústav výživy zvířat a pícninářství 5
 Mendelova zemědělská a lesnická univerzita, Ústav zahradní a krajinářské 3
 Mendelova zemědělská a lesnická univerzita, Ústav zakládání a pěstění 4
 Mendelova zemědělská a lesnická univerzita, Ústav základního zpracování 3
 Mendelova zemědělská a lesnická univerzita, Ústav základů techniky a automobilové 6
 Mendelova zemědělská a lesnická univerzita, Ústav zemědělské, potravinářské 10
 Mendelova zemědělská a lesnická univerzita, Ústav zoologie a včelařství 1
 Mendelova zemědělská a lesnická univerzita, Ústav zoologie, rybářství, 2
 Mendelova zemědělská a lesnická univerzita, Ústav šlechtění a množení 8
 Mendelova zemědělská a lesnická univerzita, Zahradnická fakulta 138
 Teesside University, Prague College 1
 Univerzita Karlova 182087
 Univerzita Karlova, Filozofická fakulta 1
 Univerzita Karlova, Katedra mediálních studií 1
 Univerzita Karlova, Pedagogická fakulta (Praha, Česko) 3
 Univerzita Karlova, Právnická fakulta 11
 Vysoká škola chemicko-technologická v Praze 1
 Vysoká škola chemicko-technologická v Praze. Fakulta chemicko-inženýrská. 1
 Vysoká škola ekonomická v Praze 43379
 Vysoká škola zemědělská 1
 Vysoká škola zemědělská a lesnická v Brně 8
 Vysoká škola zemědělská a lesnická v Brně, Lesnická fakulta 12
 Vysoká škola zemědělská a lesnická v Brně, Směr chovatelský 1
 Vysoká škola zemědělská a lesnická v Brně, Ústav ovocnické a zelinářské 1
 Vysoká škola zemědělská a lesnická v Brně, Ústav zemědělské a lesnické 1
 Vysoká škola zemědělská a lesnická v Brně, Veterinární fakulta 1
 Vysoká škola zemědělská a lesnická v Brně, Zahradnická katedra v Lednici 1
 Vysoká škola zemědělská a lesnická v Brně, Zootechnická fakulta 6
 Vysoká škola zemědělská v Brně 247
 Vysoká škola zemědělská v Brně, Agronomická fakulta 273
 Vysoká škola zemědělská v Brně, Katedra agrochemie 1
 Vysoká škola zemědělská v Brně, Katedra agrochemie a analytické chemie 1
 Vysoká škola zemědělská v Brně, Katedra agrochemie a analytické chemie, 1
 Vysoká škola zemědělská v Brně, Katedra bioklimatologie 1
 Vysoká škola zemědělská v Brně, Katedra botaniky a mikrobiologie 2
 Vysoká škola zemědělská v Brně, Katedra chovu koní, ovcí a kožešinových 5
 Vysoká škola zemědělská v Brně, Katedra chovu prasat a drůbeže 1
 Vysoká škola zemědělská v Brně, Katedra chovu skotu 2
 Vysoká škola zemědělská v Brně, Katedra ekonomiky a řízení zemědělství 2
 Vysoká škola zemědělská v Brně, Katedra geodézie a fotogrametrie 1
 Vysoká škola zemědělská v Brně, Katedra hospodářské úpravy lesa 5
 Vysoká škola zemědělská v Brně, Katedra inženýrských staveb lesnických 2
 Vysoká škola zemědělská v Brně, Katedra lesnické botaniky a fytocenologie 2
 Vysoká škola zemědělská v Brně, Katedra obecné zootechniky 4
 Vysoká škola zemědělská v Brně, Katedra ochrany lesů 3
 Vysoká škola zemědělská v Brně, Katedra ochrany lesů a myslivosti 1
 Vysoká škola zemědělská v Brně, Katedra organizace podniků a pracovních 2
 Vysoká škola zemědělská v Brně, Katedra pedagogiky 1
 Vysoká škola zemědělská v Brně, Katedra pícninářství a včelařství 1
 Vysoká škola zemědělská v Brně, Katedra pícninářství, výroby krmiv a včelařství 1
 Vysoká škola zemědělská v Brně, Katedra pícninářství, výroby krmiv a včelařství, 1
 Vysoká škola zemědělská v Brně, Katedra politické ekonomie 2
 Vysoká škola zemědělská v Brně, Katedra pro biotechnologii výroby 1
 Vysoká škola zemědělská v Brně, Katedra půdoznalství a meteorologie 5
 Vysoká škola zemědělská v Brně, Katedra půdoznalství, meteorologie a klimatologie 2
 Vysoká škola zemědělská v Brně, Katedra pěstění lesů 1
 Vysoká škola zemědělská v Brně, Katedra rostlinné výroby 5
 Vysoká škola zemědělská v Brně, Katedra rybářství a hydrobiologie 1
 Vysoká škola zemědělská v Brně, Katedra sadovnictví, krajinářství a květinářství 3
 Vysoká škola zemědělská v Brně, Katedra statistiky a matematických metod 1
 Vysoká škola zemědělská v Brně, Katedra statistiky a práva 1
 Vysoká škola zemědělská v Brně, Katedra výživy a krmení hospodářských 1
 Vysoká škola zemědělská v Brně, Katedra základní agrotechniky 6
 Vysoká škola zemědělská v Brně, Katedra zemědělské ekonomiky a organizace 2
 Vysoká škola zemědělská v Brně, Katedra zemědělské techniky 6
 Vysoká škola zemědělská v Brně, Katedra řízení zemědělství 1
 Vysoká škola zemědělská v Brně, Katedra šlechtění lesních dřevin a zalesňování 1
 Vysoká škola zemědělská v Brně, Katedra šlechtění rostlin a zahradnictví 2
 Vysoká škola zemědělská v Brně, Lesnická fakulta 18
 Vysoká škola zemědělská v Brně, Odbor zahradnický v Lednici na Moravě, 1
 Vysoká škola zemědělská v Brně, Provozně ekonomická fakulta 38
 Vysoká škola zemědělská v Brně, Ústav mechanisace 1
 Vysoká škola zemědělská v Brně, Ústav pro okrasné zahradnictví a sadovnictví 1
 Vysoká škola zemědělská v Brně, Ústav řízení a marketingu 1
 Vysoká škola zemědělská v Brně, Zahradnická fakulta 1
 Vysoká škola zemědělská v Brně, Zootechnická fakulta 2
 Vysoká škola zemědělská v Brně, Zootechnický ústav 1
 Vysoká škola zemědělská, Agronomická fakulta 9
 Vysoká škola zemědělská, Fakulta agronomická 1
 Vysoká škola zemědělská, Fakulta strojní 1
 Vysoká škola zemědělská, Provozně ekonomická fakulta 1
 České vysoké učení technické v Praze.  Stavební fakulta. 1
"""


if __name__ == "__main__":
    run()
