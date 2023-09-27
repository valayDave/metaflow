from .runtime_collector_state import _get_card_state_directory
import fcntl
import os
import functools


class CardLock:

    POSSIBLE_STATES = ["locked_by_me", "unlocked", "locked_by_other"]

    lock_base_path = None

    def __init__(self, carduuid):
        self._carduuid = carduuid
        self._lock_path = os.path.join(self.lock_base_path, self._carduuid + ".lock")
        self._lock_file = None
        self._current_state = "unlocked"

    def _try_to_acquire_lock(self):
        try:
            lock_file = open(self._lock_path, "w")
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return lock_file
        except BlockingIOError:
            return False

    @property
    def owned_by_others(self):
        return self._current_state == "locked_by_other"

    @property
    def owns_the_lock(self):
        return self._current_state == "locked_by_me"

    @property
    def seems_unlocked(self):
        return self._current_state == "unlocked"

    def test_lockability(self):
        if self.owns_the_lock:
            return True
        elif self.seems_unlocked:
            _lock_file = self._try_to_acquire_lock()
            if _lock_file:
                _lock_file.close()
                return True
            self._current_state = "locked_by_other"
            return False
        else:
            return False

    def lock(self):
        if self.owns_the_lock:
            return True
        elif self.seems_unlocked:
            self._lock_file = self._try_to_acquire_lock()
            if self._lock_file:
                self._current_state = "locked_by_me"
                return True
            else:
                self._current_state = "locked_by_other"
                return False
        else:
            return False

    def unlock(self):
        if self.owns_the_lock:
            fcntl.flock(self._lock_file, fcntl.LOCK_UN)
            self._lock_file.close()
            self._current_state = "unlocked"
            return True
        else:
            return False

    def __del__(self):
        if self.owns_the_lock:
            self.unlock()


class LockableCardDict:
    """
    This is a class that will help us ensure that when `current.card` interface is called in an outside subprocess,
    then we are able to call the `current.card` interface in outside subprocesses without any issues.

    Some constraints which the `LockableCardDict` aims to fulfill:
    
    > No two processes can lock the same card at the same time. Once a card is locked by a process, no other process can lock it.
    (Meaning the first process to lock the card locks it for-ever. No other process can lock it after that).
    
    The life-cycle of cards is as follows:
        1. Pre-task execution :
            - @card decorator instantiates a CardComponentCollector with a LockableCardDict set to `pre_task_startup=True` and empty dictionary for it's `card_component_store`
            - This ensures that new CardComponentManagers can be trivially added by the main process.
            - Once main process is done adding all cards, we run `finalize` method :
                - it will set `pre_task_startup=False` and set post_task_startup=True
                - it will instantiate the locks. Won't check if they are already possessed by others since the code is being run in the main process and
                task execution has not started yet; (All lock alteration happens once user code starts executing).
            - Upon finishing finalize `current.card` will also dump the state of `current.card` for being available to any outside subprocesses.
        2. Task Execution:
            - Main process :
                - Which ever cards the main thread update's will be locked by the main thread.
                - The cards which are not accessed / updated by the main thread, can be locked by other processes.
                - Locked cards will provide the same type of object back to the user, but the `card_proc` method will be replaced with a
                method that will warn the user that the card is locked.
            - Subprocesses:
                - calls the `get_runtime_card` method to get the `current.card` object
                    - the method will seek out the `current.card`'s state location and load the state.
                    - the state of the card specifies what ever task spec's component collector's state 
                - will be able to call render_runtime and refresh from the cli
                - `refresh` will also dump the state of the components to disk.
                - [todo] when finished will call `current.card.done()` which will dump the state of the components/data-updates to disk
                - [todo] no other processes will also be able to lock "done" cards
                - [todo] If a subprocess is gracefully shutdown, then:
                    - [todo] the card manager will call `current.card.done()` which will avoid writes from other processes
        3. Post Task Execution:
            - sub processes:
                - Should have **ideally** ended and released all locks and ran the finalize method.
                    - If not then what ever is the last component state dumped in the subprocess will be the components loaded for the card.
            - Main process:
                -  For each card,
                    - call the render method. 
                        - [todo] If the card was locked by an outside process then seek it's component state and load it.
                        - if the card was not locked by an outside process then use the in-memory component state of the card.
    """

    def __init__(
        self,
        card_component_store,
        lock_base_path,
        pre_task_startup=False,
        post_task_startup=True,
    ):
        self._card_component_store = card_component_store
        self._card_locks = {}
        self._lock_base_path = lock_base_path
        self._pre_task_startup = pre_task_startup
        self._post_task_startup = post_task_startup
        self._init_locks()

    def _init_locks(self):
        if self._pre_task_startup:
            return

        CardLock.lock_base_path = self._lock_base_path
        for carduuid in self._card_component_store:
            self._card_locks[carduuid] = CardLock(carduuid)

        if not self._post_task_startup:
            return

        # Since locks are initializing here we can test lockability here.
        # As this code may be instantiated in a subprocess outside the main process,
        # we need to ensure that when then LockableCardDict initializes, it can keeps only
        # unlocked cards in the card_component_store.
        # At this point when the code is running, the owning process of this object will
        # not have any locks that it owns since all objects are being initialized.
        for carduuid in self._card_component_store:
            self._card_locks[carduuid].test_lockability()
            if not self._card_locks[carduuid].seems_unlocked:
                self._disable_rendering_for_locked_card(carduuid)

    def finalize(self):
        # This method will only be called once by the CardComponent collector in scope of the
        # @card decorator
        self._pre_task_startup = False
        self._init_locks()
        self._post_task_startup = True

    def _disable_rendering_for_locked_card(self, carduuid):
        def _wrap_method(instance, method_name):
            def wrapper(*args, **kwargs):
                instance._warning(
                    "Card is locked by another process. "
                    "This card will not be rendered/refreshed."
                )
                return None

            # Bind the new method to the instance
            setattr(instance, method_name, wrapper)

        _wrap_method(self._card_component_store[carduuid], "_card_proc")

    def _lock_check_for_getter(self, key):
        # Here the key is uuid, so it's an internal things.
        # Users will never have access to this object
        assert key in self._card_locks, "Card not found in card store."
        if self._post_task_startup:
            if self._card_locks[key].seems_unlocked:
                if not self._card_locks[key].lock():
                    # If a card was locked then the in-memory status
                    # here will also flip to locked.
                    self._disable_rendering_for_locked_card(key)

    def _lock_check_for_setter(self, key):
        if self._post_task_startup:
            if self._card_locks[key].seems_unlocked:
                if not self._card_locks[key].lock():
                    return False
            elif self._card_locks[key].owned_by_others:
                return False
        return True

    def unlock_everything(self):
        for key in self._card_locks:
            self._card_locks[key].unlock()

    def __contains__(self, key):
        return key in self._card_component_store

    def __getitem__(self, key):
        self._lock_check_for_getter(key)
        return self._card_component_store[key]

    def __setitem__(self, key, value):
        self._card_component_store[key] = value
        if not self._lock_check_for_setter(key):
            self._disable_rendering_for_locked_card(key)

    def items(self):
        return self._card_component_store.items()

    def __len__(self):
        return len(self._card_component_store)
