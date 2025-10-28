import pytest
from abc import ABC, abstractmethod
from dagster_utils.utils.registry import Registry, RegisterSubclass


class TestRegistry:
    """Test suite for the Registry class."""

    def test_init_empty_registry(self):
        """Test that a new registry is initialized empty."""
        registry = Registry()
        assert len(registry) == 0
        assert list(registry) == []

    def test_register_single_item(self):
        """Test registering a single item."""
        registry = Registry()
        obj = "test_object"
        result = registry.register("key1", obj)
        
        assert result == obj
        assert len(registry) == 1
        assert "key1" in registry
        assert registry["key1"] == obj

    def test_register_multiple_items(self):
        """Test registering multiple items."""
        registry = Registry()
        registry.register("key1", "obj1")
        registry.register("key2", "obj2")
        registry.register("key3", "obj3")
        
        assert len(registry) == 3
        assert registry["key1"] == "obj1"
        assert registry["key2"] == "obj2"
        assert registry["key3"] == "obj3"

    def test_register_duplicate_key_raises_error(self):
        """Test that registering the same key twice raises ValueError."""
        registry = Registry()
        registry.register("duplicate", "first_object")
        
        with pytest.raises(ValueError, match=r"Name duplicate is already registered"):
            registry.register("duplicate", "second_object")

    def test_register_different_types(self):
        """Test registering objects of different types."""
        registry = Registry()
        registry.register("string", "text")
        registry.register("int", 42)
        registry.register("list", [1, 2, 3])
        registry.register("dict", {"key": "value"})
        registry.register("class", TestRegistry)
        
        assert registry["string"] == "text"
        assert registry["int"] == 42
        assert registry["list"] == [1, 2, 3]
        assert registry["dict"] == {"key": "value"}
        assert registry["class"] == TestRegistry

    def test_getitem(self):
        """Test __getitem__ method."""
        registry = Registry()
        registry.register("key", "value")
        assert registry["key"] == "value"

    def test_getitem_nonexistent_key_raises_keyerror(self):
        """Test that accessing a non-existent key raises KeyError."""
        registry = Registry()
        with pytest.raises(KeyError):
            _ = registry["nonexistent"]

    def test_setitem(self):
        """Test __setitem__ method."""
        registry = Registry()
        registry["key1"] = "value1"
        assert registry["key1"] == "value1"
        assert len(registry) == 1

    def test_setitem_override_existing(self):
        """Test that __setitem__ can override existing keys (unlike register)."""
        registry = Registry()
        registry["key"] = "original"
        registry["key"] = "updated"
        assert registry["key"] == "updated"
        assert len(registry) == 1

    def test_delitem(self):
        """Test __delitem__ method."""
        registry = Registry()
        registry["key1"] = "value1"
        registry["key2"] = "value2"
        
        del registry["key1"]
        
        assert len(registry) == 1
        assert "key1" not in registry
        assert "key2" in registry

    def test_delitem_nonexistent_raises_keyerror(self):
        """Test that deleting a non-existent key raises KeyError."""
        registry = Registry()
        with pytest.raises(KeyError):
            del registry["nonexistent"]

    def test_len(self):
        """Test __len__ method."""
        registry = Registry()
        assert len(registry) == 0
        
        registry["key1"] = "value1"
        assert len(registry) == 1
        
        registry["key2"] = "value2"
        assert len(registry) == 2
        
        del registry["key1"]
        assert len(registry) == 1

    def test_iter(self):
        """Test __iter__ method."""
        registry = Registry()
        registry["a"] = 1
        registry["b"] = 2
        registry["c"] = 3
        
        keys = list(registry)
        assert set(keys) == {"a", "b", "c"}

    def test_contains(self):
        """Test 'in' operator."""
        registry = Registry()
        registry["key"] = "value"
        
        assert "key" in registry
        assert "nonexistent" not in registry

    def test_keys(self):
        """Test keys() method from MutableMapping."""
        registry = Registry()
        registry["key1"] = "value1"
        registry["key2"] = "value2"
        
        keys = list(registry.keys())
        assert set(keys) == {"key1", "key2"}

    def test_values(self):
        """Test values() method from MutableMapping."""
        registry = Registry()
        registry["key1"] = "value1"
        registry["key2"] = "value2"
        
        values = list(registry.values())
        assert set(values) == {"value1", "value2"}

    def test_items(self):
        """Test items() method from MutableMapping."""
        registry = Registry()
        registry["key1"] = "value1"
        registry["key2"] = "value2"
        
        items = list(registry.items())
        assert set(items) == {("key1", "value1"), ("key2", "value2")}

    def test_get_with_default(self):
        """Test get() method from MutableMapping."""
        registry = Registry()
        registry["key"] = "value"
        
        assert registry.get("key") == "value"
        assert registry.get("nonexistent") is None
        assert registry.get("nonexistent", "default") == "default"

    def test_pop(self):
        """Test pop() method from MutableMapping."""
        registry = Registry()
        registry["key1"] = "value1"
        registry["key2"] = "value2"
        
        value = registry.pop("key1")
        assert value == "value1"
        assert "key1" not in registry
        assert len(registry) == 1

    def test_pop_with_default(self):
        """Test pop() method with default value."""
        registry = Registry()
        
        result = registry.pop("nonexistent", "default")
        assert result == "default"

    def test_clear(self):
        """Test clear() method from MutableMapping."""
        registry = Registry()
        registry["key1"] = "value1"
        registry["key2"] = "value2"
        
        registry.clear()
        
        assert len(registry) == 0
        assert list(registry) == []

    def test_update(self):
        """Test update() method from MutableMapping."""
        registry = Registry()
        registry["key1"] = "value1"
        
        registry.update({"key2": "value2", "key3": "value3"})
        
        assert len(registry) == 3
        assert registry["key2"] == "value2"
        assert registry["key3"] == "value3"

    def test_setdefault(self):
        """Test setdefault() method from MutableMapping."""
        registry = Registry()
        
        result = registry.setdefault("key", "default")
        assert result == "default"
        assert registry["key"] == "default"
        
        result = registry.setdefault("key", "new_value")
        assert result == "default"  # Should not change
        assert registry["key"] == "default"

    def test_register_returns_object(self):
        """Test that register returns the registered object (useful for decorators)."""
        registry = Registry()
        
        class MyClass:
            pass
        
        result = registry.register("my_class", MyClass)
        assert result is MyClass

    def test_empty_string_key(self):
        """Test that empty string can be used as a key."""
        registry = Registry()
        registry.register("", "empty_key_value")
        assert registry[""] == "empty_key_value"

    def test_none_value(self):
        """Test that None can be registered as a value."""
        registry = Registry()
        registry.register("none_key", None)
        assert registry["none_key"] is None
        assert "none_key" in registry


class TestRegisterSubclass:
    """Test suite for the RegisterSubclass mixin."""

    def test_base_class_gets_registry(self):
        """Test that a base class inheriting from RegisterSubclass gets a registry attribute."""
        class BaseClass(RegisterSubclass):
            pass
        
        assert hasattr(BaseClass, "registry")
        assert isinstance(BaseClass.registry, Registry)
        assert len(BaseClass.registry) == 0

    def test_subclass_registration(self):
        """Test that subclasses are automatically registered."""
        class BaseClass(RegisterSubclass):
            pass
        
        class SubClass1(BaseClass):
            pass
        
        class SubClass2(BaseClass):
            pass
        
        assert len(BaseClass.registry) == 2
        assert "SubClass1" in BaseClass.registry
        assert "SubClass2" in BaseClass.registry
        assert BaseClass.registry["SubClass1"] is SubClass1
        assert BaseClass.registry["SubClass2"] is SubClass2

    def test_abstract_base_class_not_registered(self):
        """Test that abstract base classes are not registered."""
        class BaseClass(RegisterSubclass):
            pass
        
        class AbstractClass(BaseClass, ABC):
            @abstractmethod
            def abstract_method(self):
                pass
        
        # AbstractClass should not be in the registry
        assert "AbstractClass" not in BaseClass.registry
        assert len(BaseClass.registry) == 0

    def test_concrete_subclass_of_abstract_registered(self):
        """Test that concrete subclasses of abstract classes are registered."""
        class BaseClass(RegisterSubclass):
            pass
        
        class AbstractClass(BaseClass, ABC):
            @abstractmethod
            def abstract_method(self):
                pass
        
        class ConcreteClass(AbstractClass):
            def abstract_method(self):
                return "implemented"
        
        assert "ConcreteClass" in BaseClass.registry
        assert "AbstractClass" not in BaseClass.registry
        assert len(BaseClass.registry) == 1

    def test_multiple_inheritance_chains(self):
        """Test that multiple inheritance chains work correctly."""
        class BaseClass(RegisterSubclass):
            pass
        
        class SubClass1(BaseClass):
            pass
        
        class SubClass2(SubClass1):
            pass
        
        class SubClass3(BaseClass):
            pass
        
        assert len(BaseClass.registry) == 3
        assert "SubClass1" in BaseClass.registry
        assert "SubClass2" in BaseClass.registry
        assert "SubClass3" in BaseClass.registry

    def test_different_base_classes_have_separate_registries(self):
        """Test that different base classes have separate registries."""
        class BaseClass1(RegisterSubclass):
            pass
        
        class BaseClass2(RegisterSubclass):
            pass
        
        class Sub1(BaseClass1):
            pass
        
        class Sub2(BaseClass2):
            pass
        
        assert len(BaseClass1.registry) == 1
        assert len(BaseClass2.registry) == 1
        assert "Sub1" in BaseClass1.registry
        assert "Sub2" in BaseClass2.registry
        assert "Sub1" not in BaseClass2.registry
        assert "Sub2" not in BaseClass1.registry

    def test_duplicate_subclass_name_raises_error(self):
        """Test that duplicate subclass names raise an error."""
        class BaseClass(RegisterSubclass):
            pass
        
        class DuplicateName(BaseClass):
            pass
        
        with pytest.raises(ValueError, match=r"Name DuplicateName is already registered"):
            class DuplicateName(BaseClass):
                pass

    def test_registry_attribute_override_warning(self):
        """Test that overriding the registry attribute in a direct subclass raises a warning."""
        global_registry = Registry()
        with pytest.warns(UserWarning, match=r"registry.*is used for internal purposes"):
            class BaseClass(RegisterSubclass):
                # Valid but custom registry attribute.
                registry = global_registry

    def test_registry_already_exists_warning(self):
        """Test warning when subclassing a class that already has RegisterSubclass."""
        class BaseClass1(RegisterSubclass):
            pass
        
        with pytest.warns(UserWarning, match=r"already a subclass.*original registry will be overridden"):
            class BaseClass2(BaseClass1, RegisterSubclass):
                pass

    def test_invalid_registry_type_raises_error(self):
        """Test that an invalid registry type raises an error."""
        class BaseClass(RegisterSubclass):
            pass
        
        # Manually set an invalid registry type
        BaseClass.registry = "invalid"
        
        with pytest.raises(TypeError, match=r"registry.*is not.*valid"):
            class SubClass(BaseClass):
                pass

    def test_access_registry_from_instance(self):
        """Test that registry can be accessed from both class and instance."""
        class BaseClass(RegisterSubclass):
            pass
        
        class SubClass(BaseClass):
            pass
        
        instance = SubClass()
        
        assert hasattr(instance, "registry")
        assert instance.registry is SubClass.registry
        assert "SubClass" in instance.registry

    def test_subclass_names_as_strings(self):
        """Test that subclass names in registry are strings matching class names."""
        class BaseClass(RegisterSubclass):
            pass
        
        class MyCustomClass(BaseClass):
            pass
        
        keys = list(BaseClass.registry.keys())
        assert "MyCustomClass" in keys
        assert BaseClass.registry["MyCustomClass"].__name__ == "MyCustomClass"

    def test_deeply_nested_inheritance(self):
        """Test deeply nested inheritance chains."""
        class BaseClass(RegisterSubclass):
            pass
        
        class Level1(BaseClass):
            pass
        
        class Level2(Level1):
            pass
        
        class Level3(Level2):
            pass
        
        class Level4(Level3):
            pass
        
        assert len(BaseClass.registry) == 4
        assert all(name in BaseClass.registry for name in ["Level1", "Level2", "Level3", "Level4"])

    def test_mixin_with_other_classes(self):
        """Test that RegisterSubclass works as a mixin with other classes."""
        class OtherMixin:
            def other_method(self):
                return "other"
        
        class BaseClass(RegisterSubclass, OtherMixin):
            pass
        
        class SubClass(BaseClass):
            pass
        
        assert hasattr(BaseClass, "registry")
        assert "SubClass" in BaseClass.registry
        assert SubClass().other_method() == "other"

    def test_registry_persistence_across_imports(self):
        """Test that registry persists and is shared across references."""
        class BaseClass(RegisterSubclass):
            pass
        
        class SubClass1(BaseClass):
            pass
        
        # Store a reference to the registry
        registry_ref = BaseClass.registry
        
        class SubClass2(BaseClass):
            pass
        
        # Registry should be the same object
        assert registry_ref is BaseClass.registry
        assert len(registry_ref) == 2

    def test_subclass_with_init_subclass_kwargs(self):
        """Test that __init_subclass__ kwargs work correctly."""
        class BaseClass(RegisterSubclass):
            def __init_subclass__(cls, custom_param=None, **kwargs):
                super().__init_subclass__(**kwargs)
                cls.custom_param = custom_param
        
        class SubClass(BaseClass, custom_param="test_value"):
            pass
        
        assert SubClass.custom_param == "test_value"
        assert "SubClass" in BaseClass.registry

    def test_empty_registry_initially(self):
        """Test that a new base class starts with an empty registry."""
        class NewBase(RegisterSubclass):
            pass
        
        assert len(NewBase.registry) == 0
        assert list(NewBase.registry) == []

    def test_subclass_instantiation(self):
        """Test that registered subclasses can be instantiated."""
        class BaseClass(RegisterSubclass):
            def __init__(self, value):
                self.value = value
        
        class SubClass(BaseClass):
            pass
        
        # Get the class from registry and instantiate it
        cls = BaseClass.registry["SubClass"]
        instance = cls(42)
        
        assert isinstance(instance, SubClass)
        assert isinstance(instance, BaseClass)
        assert instance.value == 42

    def test_abstract_method_inheritance(self):
        """Test proper abstract method handling in inheritance."""
        class BaseClass(RegisterSubclass):
            pass
        
        class AbstractMiddle(BaseClass, ABC):
            @abstractmethod
            def required_method(self):
                pass
            
            @abstractmethod
            def another_required_method(self):
                pass
        
        # This should not be registered (still abstract)
        class PartialImpl(AbstractMiddle):
            def required_method(self):
                return "implemented"
        
        # This should be registered (fully concrete)
        class FullImpl(AbstractMiddle):
            def required_method(self):
                return