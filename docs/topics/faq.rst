###
FAQ
###

1. How can I perform some debug logging within a code block?

   You can simply import the logging module within a code block. For example:

.. code-block:: yaml
:linenos:

tools:
    default:
    cores: 2
    mem: cores * 3
    rules:
        - if: True
          execute: |
            import logging
            log = logging.getLogger(__name__)
            log.debug(f"Here's what the entity looks like: {entity}")


2. How can I import custom libraries or code?

   As long as the code is available within Galaxy's virtualenv or the python path,
   it can be imported like any other package. For example:

.. code-block:: yaml
:linenos:

tools:
    default:
    cores: 2
    mem: cores * 3
    rules:
        - if: True
          execute: |
            import my_custom_module
            my_custom_module.my_func()
