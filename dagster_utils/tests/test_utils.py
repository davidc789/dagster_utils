from ..utils import cleanse_obj_name

class TestCleanseObjName:
    def test_cleanse_obj_name_should_remove_caps(self):
        assert cleanse_obj_name("HELLOWORLD") == "helloworld"
        assert cleanse_obj_name("fOo_BAr_foOd_BaR") == "foo_bar_food_bar"
        assert cleanse_obj_name("hjgFTYFty7ecVJ") == "hjgftyfty7ecvj"

    def test_cleanse_obj_name_should_remove_symbols(self):
        pass

    def test_cleanse_obj_name_should_handle_complex_cases(self):
        assert cleanse_obj_name("gHJK()^ )%gGUv &*32&*5!") == "ghjk_gguv_325"
        assert cleanse_obj_name("UIOfjd   ^&(  DQd__f=sdf1_") == "uiofjd_dqd_fsdf1_"
        assert cleanse_obj_name("hjgFTYFty7ecVJ") == "hjgftyfty7ecvj"
