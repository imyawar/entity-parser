import logging

# from chick_fil_a.CFAJsonToCsv import CFAJsonToCsv
# from chick_fil_a.CFALocation import CFALocation
# from chick_fil_a.CFAMenu import CFAMenu
from common.ActionName import ActionName
from common.ParserName import ParserName
# from daves.DavesJsonToCsv import DavesJsonToCsv
# from daves.DavesLocation import DavesLocation
# from daves.DavesMenu import DavesMenu
# from orange_theory.OrangeJsonToCsv import OrangeJsonToCsv
# from orange_theory.OrangeLocation import OrangeLocation
# from orange_theory.OrangeMenu import OrangeMenu
# from zaxbys.ZaxbysJsonToCsv import ZaxbysJsonToCsv
# from zaxbys.ZaxbysLocation import ZaxbysLocation
# from zaxbys.ZaxbysMenu import ZaxbysMenu
# from hardees.HARJsonToCsv import HARJsonToCsv
# from hardees.HARLocation import HARLocation
# from hardees.HARMenu import HARMenu
# from hardees.HARPostMenu import HARPostMenu
# from wendys.WendyJsonToCsv import WendyJsonToCsv
# from wendys.WendyLocation import WendyLocation
# from wendys.WendyMenu import WendyMenu
# from carlsjr.CJRJsonToCsv import CJRJsonToCsv
# from carlsjr.CJRLocation import CJRLocation
# from carlsjr.CJRMenu import CJRMenu
# from carlsjr.CJRPostMenu import CJRPostMenu
# from kfc.KFCJsonToCsv import KFCJsonToCsv
# from kfc.KFCLocation import KFCLocation
# from kfc.KFCMenu import KFCMenu
# from popeyes.PopeyesJsonToCsv import PopeyesJsonToCsv
# from popeyes.PopeyesLocation import PopeyesLocation
# from popeyes.PopeyesMenu import PopeyesMenu
# from popeyes.PopeyesPostMenu import PopeyesPostMenu
# from raising_canes.RCJsonToCsv import RCJsonToCsv
# from raising_canes.RCLocation import RCLocation
# from raising_canes.RCMenu import RCMenu
# from core_power_yoga.CPYLocation import CPYLocation
# from core_power_yoga.CPYMenu import CPYMenu
# from core_power_yoga.CPYJSONToCSV import CPYJSONtoCSV
# from yoga_six.YogaSixLocation import YogaSixLocation
# from yoga_six.YogaSixMenu import YogaSixMenu
# from yoga_six.YogaSixJSONToCSV import YogaSixJSONToCSV
# from solid_core.SolidCoreLocation import SolidCoreLocation
# from solid_core.SolidCoreMenu import SolidCoreMenu
# from solid_core.SolidCoreJSONToCSV import SolidCoreJSONToCSV
from common.ProcessLogs import ProcessLogs
from imtiaz.ImtiazLocation import ImtiazLocation
from imtiaz.ImtiazMenu import ImtiazMenu
from imtiaz.ImtiazJsonToCsv import ImtiazJsonToCsv
from imtiaz.ImtiazPostMenu import ImtiazPostMenu
# from metro.MetroLocation import MetroLocation
# from metro.MetroMenu import MetroMenu
# from metro.MetroJsonToCsv import MetroJsonToCsv

logging.getLogger().setLevel('INFO')

def lambda_handler(event, context):
    action = ""
    restaurant = ""
    code = 200
    response_event = {}
    if "action" in event or "parser" in event:
        action = event['action']
        restaurant = event['parser']
        msg = f"action={action}, restaurant={restaurant}"

    else:
        logging.error("Invalid event object")
        msg = "Invalid event object"
        code = 500

    parser = get_parser(action, restaurant, event, context)

    if parser is not None:
        if action == ActionName.PROCESS_LOCATION.value:
            response_event = parser.gen_location()
        elif action == ActionName.PROCESS_MENU.value:
            response_event = parser.gen_menu()
        elif action == ActionName.PROCESS_POST_MENU.value:
            response_event = parser.associate_missing_price()
        elif action == ActionName.MAKE_CSV.value:
            response_event = parser.parse_menu_csv()
        elif action == ActionName.PROCESS_LOGS.value:
            response_event = parser.process(ParserName[restaurant].name)
        else:
            logging.error("No action found")
            msg = "No parser found"
            code = 500
    else:
        logging.error("No parser found")
        msg = "No parser found"
        code = 500

    # response_event['msg'] = msg
    # response_event['code'] = code
    return response_event


def get_parser(action, parser, events, context):
    if action == ActionName.PROCESS_LOGS.value:
        return ProcessLogs(events, context)
    if parser==ParserName.imtiaz.name:
        if action == ActionName.PROCESS_LOCATION.value:
            return ImtiazLocation(events, context)
        elif action == ActionName.PROCESS_MENU.value:
            return ImtiazMenu(events, context)
        elif action == ActionName.PROCESS_POST_MENU.value:
            return ImtiazPostMenu(events, context)
        elif action == ActionName.MAKE_CSV.value:
            return ImtiazJsonToCsv(events, context)
    # if parser == ParserName.chickfila.name:
    #     if action == ActionName.PROCESS_LOCATION.value:
    #         return CFALocation(events, context)
    #     elif action == ActionName.PROCESS_MENU.value:
    #         return CFAMenu(events, context)
    #     elif action == ActionName.MAKE_CSV.value:
    #         return CFAJsonToCsv(events, context)
    # elif parser == ParserName.daves.name:
    #     if action == ActionName.PROCESS_LOCATION.value:
    #         return DavesLocation(events, context)
    #     elif action == ActionName.PROCESS_MENU.value:
    #         return DavesMenu(events, context)
    #     elif action == ActionName.MAKE_CSV.value:
    #         return DavesJsonToCsv(events, context)
    # elif parser == ParserName.zaxbys.name:
    #     if action == ActionName.PROCESS_LOCATION.value:
    #         return ZaxbysLocation(events, context)
    #     elif action == ActionName.PROCESS_MENU.value:
    #         return ZaxbysMenu(events, context)
    #     elif action == ActionName.MAKE_CSV.value:
    #         return ZaxbysJsonToCsv(events, context)
    # elif parser == ParserName.hardees.name:
    #     if action == ActionName.PROCESS_LOCATION.value:
    #         return HARLocation(events, context)
    #     elif action == ActionName.PROCESS_MENU.value:
    #         return HARMenu(events, context)
    #     elif action == ActionName.PROCESS_POST_MENU.value:
    #         return HARPostMenu(events, context)
    #     elif action == ActionName.MAKE_CSV.value:
    #         return HARJsonToCsv(events, context)
    # elif parser == ParserName.wendy.name:
    #     if action == ActionName.PROCESS_LOCATION.value:
    #         return WendyLocation(events, context)
    #     elif action == ActionName.PROCESS_MENU.value:
    #         return WendyMenu(events, context)
    #     elif action == ActionName.MAKE_CSV.value:
    #         return WendyJsonToCsv(events, context)
    # elif parser == ParserName.cjr.name:
    #     if action == ActionName.PROCESS_LOCATION.value:
    #         return CJRLocation(events, context)
    #     elif action == ActionName.PROCESS_MENU.value:
    #         return CJRMenu(events, context)
    #     elif action == ActionName.PROCESS_POST_MENU.value:
    #         return CJRPostMenu(events, context)
    #     elif action == ActionName.MAKE_CSV.value:
    #         return CJRJsonToCsv(events, context)
    # elif parser == ParserName.kfc.name:
    #     if action == ActionName.PROCESS_LOCATION.value:
    #         return KFCLocation(events, context)
    #     elif action == ActionName.PROCESS_MENU.value:
    #         return KFCMenu(events, context)
    #     elif action == ActionName.MAKE_CSV.value:
    #         return KFCJsonToCsv(events, context)
    # elif parser == ParserName.popeyes.name:
    #     if action == ActionName.PROCESS_LOCATION.value:
    #         return PopeyesLocation(events, context)
    #     elif action == ActionName.PROCESS_MENU.value:
    #         return PopeyesMenu(events, context)
    #     elif action == ActionName.PROCESS_POST_MENU.value:
    #         return PopeyesPostMenu(events, context)
    #     elif action == ActionName.MAKE_CSV.value:
    #         return PopeyesJsonToCsv(events, context)
    # elif parser == ParserName.rc.name:
    #     if action == ActionName.PROCESS_LOCATION.value:
    #         return RCLocation(events, context)
    #     elif action == ActionName.PROCESS_MENU.value:
    #         return RCMenu(events, context)
    #     elif action == ActionName.MAKE_CSV.value:
    #         return RCJsonToCsv(events, context)
    # elif parser == ParserName.cpy.name:
    #     if action == ActionName.PROCESS_LOCATION.value:
    #         return CPYLocation(events, context)
    #     elif action == ActionName.PROCESS_MENU.value:
    #         return CPYMenu(events, context)
    #     elif action == ActionName.MAKE_CSV.value:
    #         return CPYJSONtoCSV(events, context)
    # elif parser == ParserName.yogasix.name:
    #     if action == ActionName.PROCESS_LOCATION.value:
    #         return YogaSixLocation(events, context)
    #     elif action == ActionName.PROCESS_MENU.value:
    #         return YogaSixMenu(events, context)
    #     elif action == ActionName.MAKE_CSV.value:
    #         return YogaSixJSONToCSV(events, context)
    # elif parser == ParserName.solidcore.name:
    #     if action == ActionName.PROCESS_LOCATION.value:
    #         return SolidCoreLocation(events, context)
    #     elif action == ActionName.PROCESS_MENU.value:
    #         return SolidCoreMenu(events, context)
    #     elif action == ActionName.MAKE_CSV.value:
    #         return SolidCoreJSONToCSV(events, context)
    # elif parser == ParserName.orange.name:
    #     if action == ActionName.PROCESS_LOCATION.value:
    #         return OrangeLocation(events, context)
    #     elif action == ActionName.PROCESS_MENU.value:
    #         return OrangeMenu(events, context)
    #     elif action == ActionName.MAKE_CSV.value:
    #         return OrangeJsonToCsv(events, context)
    # elif parser == ParserName.imtiaz.name:
    #     if action == ActionName.PROCESS_LOCATION.value:
    #         return ImtiazLocation(events, context)
    #     elif action == ActionName.PROCESS_MENU.value:
    #         return ImtiazMenu(events, context)
    #     elif action == ActionName.MAKE_CSV.value:
    #         return ImtiazJsonToCsv(events, context)
    # elif parser == ParserName.metro.name:
    #     if action == ActionName.PROCESS_LOCATION.value:
    #         return MetroLocation(events, context)
    #     elif action == ActionName.PROCESS_MENU.value:
    #         return MetroMenu(events, context)
    #     elif action == ActionName.MAKE_CSV.value:
    #         return MetroJsonToCsv(events, context)
